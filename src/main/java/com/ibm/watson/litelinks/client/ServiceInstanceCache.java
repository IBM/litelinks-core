/*
 * Copyright 2021 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.ibm.watson.litelinks.client;

import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.client.LitelinksServiceClient.ServiceInstanceInfo;
import com.ibm.watson.litelinks.client.LoadBalancingPolicy.InclusiveLoadBalancingPolicy;
import com.ibm.watson.litelinks.client.ServiceInstance.ServiceInstanceConfig;
import com.ibm.watson.litelinks.client.ServiceInstance.State;
import com.ibm.watson.litelinks.client.ServiceRegistryClient.RegistryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.ibm.watson.litelinks.LitelinksSystemPropNames.USE_FAILING_INSTANCES;
import static com.ibm.watson.litelinks.client.ThriftClientBuilder.asRuntimeException;

/**
 * Instances of this class correspond to a single logical remote service,
 * which may comprise multiple instances.
 *
 * @param <C> client class
 */
public class ServiceInstanceCache<C> extends RegistryListener {

    private static final Logger logger = LoggerFactory.getLogger(ServiceInstanceCache.class);

    public interface ServiceClientManager<C> {
        //TODO maybe split this method into separate interface
        ServiceInstanceConfig<C> getInstanceConfig(String hostname, int port,
                long registrationTime, String version, Map<Object, Object> connConfig)
                throws Exception; // config exceptions

        C createClient(ServiceInstanceConfig<C> config, long timeoutMillis)
                throws Exception; // connection exceptions

        boolean isValid(C client);

        void close(C client); //TODO exception tbd

        // preferably via direct ping/healthcheck
        void testConnection(C client, long timeoutMillis) throws Exception;

        // as thrown either by create method or client invocation
        boolean isTransportException(Throwable t);
    }

    // used as internal sentinel only
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static final ServiceInstance<?> ALL_FAILING = new ServiceInstance(null, null, null, null);

    @SuppressWarnings("rawtypes")
    private static final ServiceInstance[] EMPTYLIST = new ServiceInstance[0];
    @SuppressWarnings("rawtypes")
    private static final ServiceInstance[][] EMPTYLISTS = { EMPTYLIST, EMPTYLIST };

    //TODO TBD 0 => indefinite
    static final int NEWCLIENT_CONN_TIMEOUT = 6000; // 6sec

    // non-static to facilitate unit tests
    protected final boolean useFailing =
            !"false".equalsIgnoreCase(System.getProperty(USE_FAILING_INSTANCES));

    private final String serviceName; // used for logging

//	private final LoadBalancer activeLB, problemLB;

    // this list contains point-in-time active service instances
    @SuppressWarnings("unchecked")
    private volatile ServiceInstance<C>[] activeList = EMPTYLIST;

    // this list contains service instances which have connection problems but are not known
    // definitively to be dead. they are periodically re-tested and only used by clients if the
    // active list is empty
    @SuppressWarnings("unchecked")
    private volatile ServiceInstance<C>[] problemList = EMPTYLIST;

    /*
     * Currently, the problemList gets used for API invocation attempts if the activeList is empty. This might be
     * simplified so that in that case invocations aren't attempted and fail immediately. Doing this would
     * obviate the need for this slightly messy copy-on-write coordination.
     */

    // this is a volatile ref to an array containing the arrays from both COW lists; used to
    // obtain a consistent view across both without locking
    private volatile Object[][] bothLists = EMPTYLISTS;

    // this is the activeList's 'internal' write-lock, used make updates spanning both lists atomic
    private final ReentrantLock listsLock = new ReentrantLock();

    // used for notifiying waiting clients of availability
    private final Condition newInstanceCond = listsLock.newCondition();

    private final Map<String, ServiceInstance<C>> serviceInstances = new ConcurrentHashMap<>();

    private final ServiceClientManager<C> clientManager;
    private volatile Exception configException;

    public ServiceInstanceCache(String name, /*LoadBalancingPolicy lbp,*/ ServiceClientManager<C> clientManager) {
//		this.activeLB = lbp.getLoadBalancer();
//		this.problemLB = lbp.getLoadBalancer();
        this.clientManager = clientManager;
		this.serviceName = name;
    }

    /**
     * for logging only
     */
    protected String getServiceName() {
        return serviceName;
    }

    protected ServiceClientManager<C> getClientManager() {
        return clientManager;
    }

    /**
     * @return null if config error/invalid (e.g. svc type mismatch, bad local SSL config, etc)
     */
    protected ServiceInstanceConfig<C> makeAndValidateConfig(String hostname, int port, String version,
            long registrationTime, Map<Object, Object> connConfig, String forId) { //throws Exception {
        try {
            final ServiceInstanceConfig<C> sic = clientManager.getInstanceConfig(hostname, port,
                    registrationTime, version, connConfig);
            configException = null;
            return sic;
        } catch (Exception e) {
            listsLock.lock();
            try {
                configException = e; // notify in case user is waiting on service availability
                newInstanceCond.signalAll();
            } finally {
                listsLock.unlock();
            }
//			throw e;
            logger.error("Client config error for discovered instance "
                         + "(serviceName=" + serviceName + "): " + forId, e);
            return null;
        }
    }

    @Override
    public void serverAdded(String hostname, int port, long registrationTime, String version,
            String key, String instanceId, Map<Object, Object> connConfig, ServiceRegistryClient source) {
        final ServiceInstanceConfig<C> sic = makeAndValidateConfig(hostname, port,
                version, registrationTime, connConfig, key);
        if (sic == null) {
            return; // config error
        }
        ServiceInstance<C> si = serviceInstances.get(key);
        if (si == null) {
            serviceInstances.put(key, si = new ServiceInstance<C>(instanceId, sic, this, source));
            final ReentrantLock lock = listsLock;
            lock.lock();
            try {
                activeList = add(activeList, si); //TODO maybe insert in ordered place
            } finally {
                lock.unlock();
            }
            updateJointRef(true);
        } else {
            si.updateConfig(sic);
        }
    }

    //	@Override
    @Override
    public void serverRemoved(String id) {
        ServiceInstance<C> toRemove = serviceInstances.remove(id);
        if (toRemove != null) {
            removeFromLists(toRemove, false);
        }
    }

    //	@Override
    @Override
    public void serversAddedAndOrRemoved(Server[] servers, String[] removedIds,
            ServiceRegistryClient source) {
        List<ServiceInstance<C>> toRem = null, toAdd = null;
        if (removedIds != null) {
            for (String id : removedIds) {
                final ServiceInstance<C> rem = serviceInstances.remove(id);
                if (rem != null) {
                    (toRem != null? toRem : (toRem = new LinkedList<>())).add(rem);
                }
            }
        }
        if (servers != null && servers.length > 0) {
            for (Server s : servers) {
                final ServiceInstanceConfig<C> sic = makeAndValidateConfig(s.hostname,
                        s.port, s.version, s.registrationTime, s.connConfig, s.key);
                if (sic == null) {
                    continue; // config error
                }
                ServiceInstance<C> si = serviceInstances.get(s.key);
                if (si == null) {
                    serviceInstances.put(s.key, si = new ServiceInstance<C>(s.instanceId, sic, this, source));
                    (toAdd != null? toAdd : (toAdd = new LinkedList<>())).add(si);
                } else {
                    si.updateConfig(sic);
                }
            }
        }
        if (toAdd != null) {
            final ReentrantLock lock = listsLock;
            lock.lock();
            try {
                activeList = addAll(activeList, toAdd);
            } finally {
                lock.unlock();
            }
        }
        if (toRem != null) {
            removeFromLists(toRem, toAdd != null); // this updates jointref
        } else if (toAdd != null) {
            updateJointRef(true);
        }
    }

    //	@Override
    @Override
    public void refreshServerList(Server[] servers, ServiceRegistryClient source) {
        final Set<String> toRemove = new HashSet<>(Maps.filterEntries(serviceInstances,
                e -> e.getValue().source == source).keySet());
        for (Server s : servers) {
            toRemove.remove(s.key);
        }
        serversAddedAndOrRemoved(servers, toRemove.toArray(new String[toRemove.size()]), source);
    }

    private void removeFromLists(ServiceInstance<C> toRemove, boolean prevAdds) {
        final ReentrantLock lock = listsLock;
        State prevState;
        lock.lock();
        try {
            activeList = remove(activeList, toRemove);
            problemList = remove(problemList, toRemove);
            updateJointRef(prevAdds);
            prevState = toRemove.setState(State.INACTIVE);
        } finally {
            lock.unlock();
        }
        if (prevState != State.INACTIVE) {
            toRemove.close();
        }
    }

    private void removeFromLists(Collection<ServiceInstance<C>> toRemove, boolean prevAdds) {
        final ReentrantLock lock = listsLock;
        final Set<ServiceInstance<C>> toClose = new HashSet<>();
        lock.lock();
        try {
            activeList = removeAll(activeList, toRemove);
            problemList = removeAll(problemList, toRemove);
            updateJointRef(prevAdds);
            for (ServiceInstance<C> si : toRemove) {
                if (si.setState(State.INACTIVE) != State.INACTIVE) {
                    toClose.add(si);
                }
            }
        } finally {
            lock.unlock();
        }
        for (ServiceInstance<C> si : toClose) {
            si.close();
        }
    }

    // called by contained service instances to indicate when they change
    // between active and failed state
    protected boolean notifyFailed(ServiceInstance<C> si, boolean failed) {
        if (si.isActive() != failed) {
            return false;
        }
        final ReentrantLock lock = listsLock;
        lock.lock();
        try {
            boolean changed = failed?
                    si.changeState(State.ACTIVE, State.FAILING) :
                    si.changeState(State.FAILING, State.ACTIVE);
            if (changed) {
                problemList = failed? add(problemList, si) : remove(problemList, si);
                activeList = failed? remove(activeList, si) : add(activeList, si);
                updateJointRef(false);
            }
            return changed;
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param addition whether this update might include new instances -
     *                 if true, threads waiting on availability will be notified
     */
    private void updateJointRef(boolean addition) {
        final ReentrantLock lock = listsLock;
        Object[][] listsBefore, listsAfter;
        lock.lock();
        try {
            if ((listsBefore = bothLists) == null) {
                return; // closed, ignore
            }
            bothLists = listsAfter = new Object[][] { activeList, problemList };
            if (addition) {
                newInstanceCond.signalAll();
            }
            if (!listeners.isEmpty()) {
                List<ServiceInstanceInfo> newInfos = getServiceInstanceInfo(listsAfter, false);
                boolean availableBefore = availableAndValid(listsBefore, false),
                        availableNow = !newInfos.isEmpty();
                notifyListeners(availableBefore == availableNow? null : availableNow, newInfos);
            }
        } finally {
            lock.unlock();
        }
        logInstances(listsBefore, listsAfter);
    }

    private void logInstances(Object[][] listsBefore, Object[][] listsAfter) {
        Object[] al = listsAfter[0], pl = listsAfter[1], alb = listsBefore[0], plb = listsBefore[1];
        int a = al.length, p = pl.length, t = a + p;
        List<Object> nw = null, gone = new LinkedList<>(), chg = new LinkedList<>();
        for (Object si : alb) {
            if (!contains(al, si)) {
                (contains(pl, si)? chg : gone).add(si);
            }
        }
        int activeGone = gone.size();
        for (Object si : plb) {
            if (!contains(pl, si)) {
                (contains(al, si)? chg : gone).add(si);
            }
        }
        int totalGone = gone.size();
        if (a > alb.length - activeGone) {
            for (Object si : al) {
                if (!contains(alb, si) && !chg.contains(si)) {
                    (nw != null? nw : (nw = new LinkedList<>())).add(si);
                }
            }
        }
        if (p > plb.length + activeGone - totalGone) {
            for (Object si : pl) {
                if (!contains(plb, si) && !chg.contains(si)) {
                    (nw != null? nw : (nw = new LinkedList<>())).add(si);
                }
            }
        }
        StringBuilder sb = new StringBuilder("Service " + serviceName + ": " + a + "/" + t + " instances active |");
        int totalChanged = chg.size();
        if (nw != null) {
            sb.append(" new(").append(nw.size()).append(")=").append(nw);
        }
        if (totalGone > 0) {
            sb.append(" gone(").append(totalGone).append(")=").append(gone);
        }
        if (totalChanged > 0) {
            sb.append(" changed(").append(totalChanged).append(")=").append(chg);
        }
        logger.info(sb.toString());
    }

    protected static boolean contains(Object[] arr, Object element) {
        for (Object e : arr) {
            if (e == element) {
                return true;
            }
        }
        return false;
    }

    static class ListenerWithExecutor {
        final AvailabilityListener listener;
        final Executor executor;

        public ListenerWithExecutor(AvailabilityListener listener, Executor executor) {
            this.listener = listener;
            this.executor = executor;
        }
    }

    private final List<ListenerWithExecutor> listeners = new CopyOnWriteArrayList<>();

    void addListeners(Collection<ListenerWithExecutor> listenerWithExecutors) {
        //TODO should listeners receive a callback indicating initial state?
        listeners.addAll(listenerWithExecutors);
    }

    void removeListeners(Collection<ListenerWithExecutor> listenerWithExecutors) {
        listeners.removeAll(listenerWithExecutors);
    }

    private void notifyListeners(final Boolean availabilityChange, final List<ServiceInstanceInfo> newInfos) {
        for (ListenerWithExecutor lwe : listeners) {
            final AvailabilityListener listener = lwe.listener;
            lwe.executor.execute(() -> {
                if (availabilityChange != null) {
                    listener.availabilityChanged(serviceName, availabilityChange);
                }
                listener.instancesChanged(serviceName, newInfos);
            });
        }
    }

    private enum LBType {
        ACTIVE, PROBLEM
    }

    @SuppressWarnings("serial")
    static class Balancers extends EnumMap<LBType, LoadBalancer> {
        final boolean inclusive;

        public Balancers(LoadBalancingPolicy lbPolicy) {
            super(LBType.class);
            inclusive = lbPolicy instanceof InclusiveLoadBalancingPolicy;
            for (LBType type : LBType.values()) {
                put(type, lbPolicy.getLoadBalancer());
            }
        }
    }

    /**
     * @return the next {@link ServiceInstance} to try, null if there
     * isn't one (none available), or {@link #ALL_FAILING} if there
     * are none available that aren't in a failing state
     * (see also {@link LitelinksSystemPropNames#USE_FAILING_INSTANCES})
     */
    @SuppressWarnings("unchecked")
    protected ServiceInstance<C> getNextServiceInstance(Balancers balancers,
            String method, Object[] args) {
        final Object[][] arrays = bothLists;
        if (arrays == null) {
            throw new ClientClosedException("closed");
        }
        Object[] array = arrays[0];
        int num = array.length;
        if (num > 0) {
            ServiceInstance<C> si = num == 1 && balancers.inclusive? (ServiceInstance<C>) array[0]
                    : balancers.get(LBType.ACTIVE).getNext(array, method, args);
            if (si != null || balancers.inclusive) {
                return si;
            }
        }
        array = arrays[1];
        num = array.length;
        if (num == 0) { // no instances :(
            throwConfigExceptionIfSet();
            return null;
        } else if (!useFailing) {
            return (ServiceInstance<C>) ALL_FAILING; // no non-problem instances
        }
        return num == 1 && balancers.inclusive? (ServiceInstance<C>) array[0]
                : balancers.get(LBType.PROBLEM).getNext(array, method, args);
    }

    protected boolean awaitAvailable(long timeoutMillis) throws InterruptedException {
        if (available()) {
            return true;
        }
        if (timeoutMillis <= 0l) {
            return false;
        }
        long deadlineNanos = System.nanoTime() + timeoutMillis * 1000_000L;
        final ReentrantLock lock = listsLock;
        if (!lock.tryLock(timeoutMillis, TimeUnit.MILLISECONDS)) {
            return false;
        }
        try {
            long remain = deadlineNanos - System.nanoTime();
            while (!available()) {
                if (remain <= 0l) {
                    return false;
                }
                remain = newInstanceCond.awaitNanos(remain);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    private boolean available() {
        final Object[][] arrays = bothLists;
        if (arrays == null) {
            throw new ClientClosedException("closed");
        }
        return availableAndValid(arrays, true);
    }

    @SuppressWarnings("unchecked")
    void close() {
        if (bothLists == null) {
            return; // already closed, ignore
        }
        final ReentrantLock lock = listsLock;
        lock.lock();
        try {
            Object[][] arrays = bothLists;
            if (arrays == null) {
                return;
            }
            bothLists = null;
            newInstanceCond.signalAll();
            for (Object[] list : arrays) {
                for (Object si : list) {
                    if (si != null) {
                        ((ServiceInstance<C>) si).close();
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    boolean isClosed() {
        return bothLists == null;
    }

    // just for instrumentation
    public List<ServiceInstanceInfo> getServiceInstanceInfo() {
        return getServiceInstanceInfo(bothLists, true);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public List<ServiceInstanceInfo> getServiceInstanceInfo(Object[][] arrays, boolean checkConfig) {
        if (arrays == null) {
            throw new ClientClosedException("closed");
        }
        boolean empty0 = arrays[0].length == 0, empty1 = arrays[1].length == 0;
        if (empty0 && empty1) {
            if (checkConfig) {
                throwConfigExceptionIfSet();
            }
            return Collections.emptyList();
        }
        Object[] arr = empty0? arrays[1] : empty1? arrays[0]
                : ObjectArrays.concat(arrays[0], arrays[1], Object.class);
        return (List<ServiceInstanceInfo>) (List) Collections.unmodifiableList(Arrays.asList(arr));
    }

    private boolean availableAndValid(Object[][] arrays, boolean checkConfig) {
        if (arrays[0].length > 0 || arrays[1].length > 0) {
            return true;
        }
        if (checkConfig) {
            throwConfigExceptionIfSet();
        }
        return false;
    }

    private void throwConfigExceptionIfSet() {
        Exception ce = configException;
        if (ce != null) {
            throw asRuntimeException(ce);
        }
    }

    // ------- COW array methods. Must be used within listsLock, provided list isn't modified

    static <T> T[] add(T[] list, T item) {
        T[] newList = Arrays.copyOf(list, list.length + 1);
        newList[list.length] = item;
        return newList;
    }

    static <T> T[] addAll(T[] list, Collection<T> items) {
        int size = items.size();
        if (size == 0) {
            return list;
        }
        T[] newList = Arrays.copyOf(list, list.length + size);
        Iterator<T> it = items.iterator();
        for (int i = list.length; i < newList.length; i++) {
            newList[i] = it.next();
        }
        return newList;
    }

    static <T> ServiceInstance<T>[] remove(ServiceInstance<T>[] list, ServiceInstance<T> item) {
        for (int i = 0; i < list.length; i++) {
            if (Objects.equals(list[i], item)) {
                @SuppressWarnings("unchecked")
                ServiceInstance<T>[] newList = new ServiceInstance[list.length - 1];
                if (i != 0) {
                    System.arraycopy(list, 0, newList, 0, i);
                }
                if (i != newList.length) {
                    System.arraycopy(list, i + 1, newList, i, newList.length - i);
                }
                return newList;
            }
        }
        return list;
    }

    static <T> ServiceInstance<T>[] removeAll(ServiceInstance<T>[] list, Collection<ServiceInstance<T>> items) {
        int count = 0;
        for (int i = 0; i < list.length; i++) {
            if (items.contains(list[i])) {
                count++;
            }
        }
        if (count == 0) {
            return list;
        }
        @SuppressWarnings("unchecked")
        ServiceInstance<T>[] newList = new ServiceInstance[list.length - count];
        int i = 0;
        for (ServiceInstance<T> si : list) if (items.contains(si)) newList[i++] = si;
        return newList;
    }

}
