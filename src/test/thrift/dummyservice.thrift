namespace java com.ibm.watson.litelinks.test.thrift

	exception TestException {
    	1: string message;
	}
	
	exception InstanceFailingException {
    	1: string message
	}
	
	enum DummyEnum {
		OPTION1,
		OPTION2
	}
	
	union DummyUnion {
		1: string stringField
		2: i32 intField
	}
	
	struct DummyStruct {
		1: bool boolField,
		2: double doubleField,
		3: string stringField = "defaultString",
		4: DummyEnum enumField,
		5: list<string> listField,
		6: map<i32,DummyEnum> mapField,
		7: set<i64> setField,
		8: required i32 requiredIntField
		9: i64 longField
		10: binary binaryField
	}



service DummyService {
	string method_one(1:string arg1, 2:DummyStruct arg2, 3: bool arg3) throws (1:TestException error, 2:InstanceFailingException ife)
	DummyStruct method_two(1: i32 arg1, 2: string arg2, 3: binary arg3) throws (1:InstanceFailingException ife)
}

service DummyService2 {
	string just_one_method(1:string input)
}

service DummyService3 extends DummyService {
	i32 subclass_method(1: bool input)
}

/* This is a copy of DummyService with a different name, for testing the
 * service class name aliasing functionality.
 */
service DummyServiceAlias {
	string method_one(1:string arg1, 2:DummyStruct arg2, 3: bool arg3) throws (1:TestException error, 2:InstanceFailingException ife)
	DummyStruct method_two(1: i32 arg1, 2: string arg2, 3: binary arg3) throws (1:InstanceFailingException ife)
}