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

/**
 * This file is identical to the thrift 0.13.0 version of this class,
 * apart from additional Exception details included in returned TExceptions
 * (safe since litelinks is only used for internal communication)
 */
package org.apache.thrift;

import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ProcessFunction<I, T extends TBase> {
  private final String methodName;

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessFunction.class.getName());
  static { LOGGER.info("Litelinks-modified "+ProcessFunction.class.getName()+" class loaded"); }

  public ProcessFunction(String methodName) {
    this.methodName = methodName;
  }

  public final void process(int seqid, TProtocol iprot, TProtocol oprot, I iface) throws TException {
    T args = getEmptyArgsInstance();
    try {
      args.read(iprot);
    } catch (TProtocolException e) {
      iprot.readMessageEnd();
      TApplicationException x = new TApplicationException(TApplicationException.PROTOCOL_ERROR, e.getMessage());
      oprot.writeMessageBegin(new TMessage(getMethodName(), TMessageType.EXCEPTION, seqid));
      x.write(oprot);
      oprot.writeMessageEnd();
      oprot.getTransport().flush();
      return;
    }
    iprot.readMessageEnd();
    TSerializable result = null;
    byte msgType = TMessageType.REPLY;

    try {
      result = getResult(iface, args);
    } catch (TTransportException ex) {
      LOGGER.error("Transport error while processing " + getMethodName(), ex);
      throw ex;
    } catch (TApplicationException ex) {
      LOGGER.error("Internal application error processing " + getMethodName(), ex);
      result = ex;
      msgType = TMessageType.EXCEPTION;
    } catch (Exception ex) {
      LOGGER.error("Internal error processing " + getMethodName(), ex);
      if(rethrowUnhandledExceptions()) throw new RuntimeException(ex.getMessage(), ex);
      if(!isOneway()) {
        result = new TApplicationException(TApplicationException.INTERNAL_ERROR,
            "Internal error processing " + getMethodName() + ": " + ex);
        msgType = TMessageType.EXCEPTION;
      }
    }

    if(!isOneway()) {
      oprot.writeMessageBegin(new TMessage(getMethodName(), msgType, seqid));
      result.write(oprot);
      oprot.writeMessageEnd();
      oprot.getTransport().flush();
    }
  }

  private void handleException(int seqid, TProtocol oprot) throws TException {
    if (!isOneway()) {
      TApplicationException x = new TApplicationException(TApplicationException.INTERNAL_ERROR,
        "Internal error processing " + getMethodName());
      oprot.writeMessageBegin(new TMessage(getMethodName(), TMessageType.EXCEPTION, seqid));
      x.write(oprot);
      oprot.writeMessageEnd();
      oprot.getTransport().flush();
    }
  }

  protected boolean rethrowUnhandledExceptions(){
    return false;
  }

  protected abstract boolean isOneway();

  public abstract TBase getResult(I iface, T args) throws TException;

  public abstract T getEmptyArgsInstance();

  public String getMethodName() {
    return methodName;
  }
}
