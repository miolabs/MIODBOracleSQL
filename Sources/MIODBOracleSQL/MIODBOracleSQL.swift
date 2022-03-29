
import Foundation
import MIOCore
import MIODB
import CLibOracle

enum MIODBOracleSQLError: Error {
    case fatalError(_ msg: String)
    case statusError(_ status: Int32, error:String)
}

extension MIODBOracleSQLError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case let .fatalError(msg):
            return "[MIODBOracleSQLError] Fatal error \"\(msg)\"."
        case let .statusError(status, error):
            switch(status){
                case OCI_SUCCESS_WITH_INFO: return "Error - OCI_SUCCESS_WITH_INFO"
                case OCI_NEED_DATA:         return "Error - OCI_NEED_DATA"
                case OCI_NO_DATA:           return "Error - OCI_NODATA"
                case OCI_INVALID_HANDLE:    return "Error - OCI_INVALID_HANDLE"
                case OCI_STILL_EXECUTING:   return "Error - OCI_STILL_EXECUTE"
                case OCI_CONTINUE:          return "Error - OCI_CONTINUE"
                case OCI_ERROR:             return "Error - \(error)"
            default: return "Unknown"
            }
        }
    }
}


open class MIODBOracleSQL: MIODB, MDBQueryProtocol
{
    let defaultPort:Int32 = 1521
    let defaultUser = "root"
    let defaultDatabase = "public"
    
    func makeCString(_ str: String) -> UnsafeMutablePointer<UInt8> {
        var utf8 = Array(str.utf8)
        utf8.append(0)  // adds null character
        let count = utf8.count
        let result = UnsafeMutableBufferPointer<UInt8>.allocate(capacity: count)
        _ = result.initialize(from: utf8)
        return result.baseAddress!
    }
        
    var env:UnsafeMutableRawPointer?
    var err:UnsafeMutableRawPointer?
    var srv:UnsafeMutableRawPointer?
    var svc:UnsafeMutableRawPointer?
    var auth:UnsafeMutableRawPointer?
    
            
    open override func connect() throws {
        if port == nil { port = defaultPort }
        if user == nil { user = defaultUser }
        if database == nil { database = defaultDatabase }
                
        var _env:OpaquePointer?
        let errorCode = OCIEnvCreate(&_env, ub4(OCI_DEFAULT), nil, nil, nil, nil, 0, nil)
        if errorCode != 0 {
            print("OCIEnvCreate failed with errcode = \(errorCode).")
        }
        env = UnsafeMutableRawPointer(_env!)
                        
        /* server contexts */
        OCIHandleAlloc( env, &err, ub4(OCI_HTYPE_ERROR), 0, nil)
        
        OCIHandleAlloc( env, &srv, ub4(OCI_HTYPE_SERVER), 0, nil)
        
        let con_str = "//\(host!):\(port!)/\(database!)"
        var status = OCIServerAttach( OpaquePointer(srv), OpaquePointer(err), makeCString(con_str), sb4(con_str.count), 0 )
        LogStatus(status, err, "SERVER ATTACH")
        if status != OCI_SUCCESS {
            throw MIODBOracleSQLError.statusError(status, error: "Can't attach server session")
        }
        
        OCIHandleAlloc( UnsafeRawPointer(env), &svc, ub4(OCI_HTYPE_SVCCTX), 0, nil)
        
        /* set attribute server context in the service context */
        OCIAttrSet( svc, ub4(OCI_HTYPE_SVCCTX), srv, 0, ub4(OCI_ATTR_SERVER), OpaquePointer(err))
                
        OCIHandleAlloc( env, &auth, ub4(OCI_HTYPE_SESSION), 0, nil)

        OCIAttrSet( auth, ub4(OCI_HTYPE_SESSION), makeCString(user!), ub4(user!.count), ub4(OCI_ATTR_USERNAME), OpaquePointer(err))

        OCIAttrSet( auth, ub4(OCI_HTYPE_SESSION), makeCString(password!), ub4(password!.count), ub4(OCI_ATTR_PASSWORD), OpaquePointer(err))

        status = OCISessionBegin ( OpaquePointer(svc), OpaquePointer(err), OpaquePointer(auth), ub4(OCI_CRED_RDBMS), ub4(OCI_DEFAULT) )
        LogStatus(status, err, "SESSION BEGIN")
        if status != OCI_SUCCESS {
            throw MIODBOracleSQLError.statusError(status, error: "Can't start server session begin")
        }

        
        OCIAttrSet(svc, ub4(OCI_HTYPE_SVCCTX), auth, 0, ub4(OCI_ATTR_SESSION), OpaquePointer(err) )
    }
        
    open override func disconnect() {

        // Close connection
        if svc != nil && err != nil && auth != nil {
            let status = OCISessionEnd( OpaquePointer(svc), OpaquePointer(err), OpaquePointer(auth), 0 )
            LogStatus(status, err, "SESSION END")
        }
        
        if srv != nil && err != nil {
            let status = OCIServerDetach( OpaquePointer(srv), OpaquePointer(err), ub4(OCI_DEFAULT) )
            LogStatus(status, err, "SERVER DETACH")
        }
        
        // Free handles
        if srv  != nil { OCIHandleFree( srv, ub4(OCI_HTYPE_SERVER) ) }
        if svc  != nil { OCIHandleFree( svc, ub4(OCI_HTYPE_SVCCTX) ) }
        if err  != nil { OCIHandleFree( err, ub4(OCI_HTYPE_ERROR) ) }
        if auth != nil { OCIHandleFree( auth, ub4(OCI_HTYPE_SESSION) ) }
        if env  != nil { OCIHandleFree( UnsafeMutableRawPointer(env), ub4(OCI_HTYPE_ENV) ) }
        
        // Terminate
        OCITerminate( ub4(OCI_DEFAULT) )
        
        srv  = nil
        svc  = nil
        err  = nil
        auth = nil
        env  = nil
    }
    
    @discardableResult open override func execute(_ query: MDBQuery) throws -> [[String : Any]]? {
        query.delegate = self
        return try super.execute( query )
    }
    
    @discardableResult open override func executeQueryString(_ query:String) throws -> [[String : Any]]? {
        
        print("QUERY: \(query)")
        
        var items:[[String : Any]]?
//        try MIOCoreAutoReleasePool {
            items = try _executeQueryString(query)
//        }
        
        return items
    }
    
    @discardableResult open func _executeQueryString(_ query:String) throws -> [[String : Any]]? {
                        
//        var stmt:UnsafeMutableRawPointer?
//        var status = OCIHandleAlloc( env, &stmt, ub4(OCI_HTYPE_STMT), 0, nil)
//        LogStatus(status, err)

        var stmt:OpaquePointer?
        var status = OCIStmtPrepare2( OpaquePointer(svc), &stmt, OpaquePointer(err), makeCString(query), ub4(query.count), nil, 0, ub4(OCI_NTV_SYNTAX), ub4(OCI_DEFAULT) )
        LogStatus(status, err, "STMT PREPARE")
        if status != OCI_SUCCESS {
            throw MIODBOracleSQLError.statusError(status, error: "Can't STMT prepare")
        }
        
        var type = 0
        var type_len = ub4(0)
        status = OCIAttrGet( UnsafeRawPointer(stmt), ub4(OCI_HTYPE_STMT), &type, &type_len, ub4(OCI_ATTR_STMT_TYPE), OpaquePointer(err) )
        LogStatus(status, err, "STMT GET")
        if status != OCI_SUCCESS {
            throw MIODBOracleSQLError.statusError(status, error: "Can't STMT get")
        }
        
        /* execute and fetch */
        
        func isSuccess () -> Bool { return status == OCI_SUCCESS || status == OCI_SUCCESS_WITH_INFO }
        
        status = OCIStmtExecute( OpaquePointer(svc), stmt, OpaquePointer(err), type == OCI_STMT_SELECT ? 0 : 1, 0, nil, nil, ub4(OCI_DEFAULT) )
        LogStatus(status, err, "STMT EXECUTE")
                        
        if (status == OCI_NO_DATA) { return [] }
        
        if !isSuccess() {
            throw MIODBOracleSQLError.statusError(status, error: "Can't STMT execute")
        }
        
        /* Loop only if a descriptor was successfully retrieved for
           current position, starting at 1 */
        var count = ub4(1)
        var param:UnsafeMutableRawPointer?
        status = OCIParamGet( UnsafeRawPointer(stmt), ub4(OCI_HTYPE_STMT), OpaquePointer(err), &param, count)
        LogStatus(status, err, "GET PARAM")
        
//        if !isSuccess() {
//            throw MIODBOracleSQLError.statusError(status, error: "Can't GET PARAM")
//        }

        
        var fields:[Field] = []
        while (status == OCI_SUCCESS) {
            fields.append ( Field(index: count, stmt: stmt, param: param, env:OpaquePointer(env), err: err) )
            count += 1
            status = OCIParamGet( UnsafeRawPointer(stmt), ub4(OCI_HTYPE_STMT), OpaquePointer(err), &param, count)
            LogStatus(status, err, "GET FIELD")
            
//            if !isSuccess() {
//                throw MIODBOracleSQLError.statusError(status, error: "Can't GET FIELD")
//            }

        }
        
        var items:[[String : Any]] = []
        
        status = OCI_SUCCESS
        
        while status == OCI_SUCCESS {
            status = OCIStmtFetch2( stmt, OpaquePointer(err), 1, ub2(OCI_FETCH_NEXT), 0, ub4(OCI_DEFAULT) )
            LogStatus(status, err, "FETCH")
            if status == OCI_NO_DATA || !isSuccess() { break }
            
            var item:[String:Any] = [:]
            for f in fields {
                item[f.name] = f.value
            }
            
            items.append(item)
        }
                                                                                
        return items
    }
        
    open override func changeScheme(_ scheme:String?) throws {
//        if connection == nil {
//            throw MIODBPostgreSQLError.fatalError("Could not change the scheme. The connection is nil")
//        }
//
//        if scheme != nil {
//            try executeQueryString("SET search_path TO \(scheme!), public")
//            self.scheme = scheme!
//        }
    }
    
    public func upsert(table: String, values: [(key: String, value: MDBValue)], conflict: String, returning: [String]) -> String? {
        guard let conflict_value = values.first( where: { kv in kv.key == conflict } ) else {
            return nil
        }
        
        let insert_keys   = values.map{ kv in MDBValue( fromField: kv.key ).value }.joined(separator: ",")
        let insert_values = values.map{ kv in kv.value.value }.joined(separator: ",")
        let update_set    = values.filter{ kv in kv.key != conflict }
                                  .map{ kv in "\(MDBValue( fromField: kv.key ).value) = \(kv.value.value)" }
                                  .joined( separator: "," )

        return """
                merge into \(table) using dual on (\(conflict) = \(conflict_value.value.value))
                 when not matched then insert (\(insert_keys)) values (\(insert_values))
                     when matched then update set \(update_set)
               """
    }
}



class Field {
    
    var name:String = "UNKNOWN"
    var value:Any? { get { return convertValue() } }

    var _name:UnsafeMutablePointer<UInt8>?
    var _value:UnsafeMutableBufferPointer<UInt8>?
    var _value_number:UnsafeMutablePointer<OCINumber>?
    
    var param:UnsafeMutableRawPointer?
    var name_len = ub4(0)
    var value_len = ub2(0)
    var define:OpaquePointer?
    var inPointer = sb2(0)
    var len = ub2(0)
    var rcode = ub2(0)
    var type = ub2(0)
    var index = ub4(0)
    var err:UnsafeMutableRawPointer?

    var env:OpaquePointer?
    
    init(index:ub4, stmt:OpaquePointer?, param:UnsafeRawPointer?, env: OpaquePointer?, err:UnsafeMutableRawPointer?) {
                      
        self.index = index
        self.env = env
        self.err = err
        
      /*
       * Retrieve field name.
       */

        var status = OCIAttrGet( param, ub4(OCI_DTYPE_PARAM), &_name, &name_len, ub4(OCI_ATTR_NAME), OpaquePointer(err) )
        LogStatus(status, err, "GET FIELD")
        if status != OCI_SUCCESS { return }

        name = String(cString: _name!)
                
        status = OCIAttrGet( param, ub4(OCI_DTYPE_PARAM), &type, nil, ub4(OCI_ATTR_DATA_TYPE), OpaquePointer(err) )
        LogStatus(status, err, "GET TYPE")
        if status != OCI_SUCCESS { return }
        
        /*
        * And field width.
        */

        status = OCIAttrGet( param, ub4(OCI_DTYPE_PARAM), &value_len, nil, ub4(OCI_ATTR_DATA_SIZE), OpaquePointer(err) )
        LogStatus(status, err, "GET VALUE LEN")
        if status != OCI_SUCCESS { return }
                        
//        var utf8 = Array("".utf8)
//        utf8.append(0)  // adds null character
//        let result = UnsafeMutableBufferPointer<UInt8>.allocate(capacity: ( Int(value_len) + 1) )
//        _ = result.initialize(from: utf8)
//        _value = result.baseAddress!
                
//        if type == 1 {
            _value = UnsafeMutableBufferPointer<UInt8>.allocate(capacity: Int(1024))
        status = OCIDefineByPos( stmt, &define, OpaquePointer(err), index, _value!.baseAddress, sb4(1024), ub2(SQLT_STR), nil, &len, &rcode, ub4(OCI_DEFAULT) )
             LogStatus(status, err, "DEFINE BY POS")

//        }
//        else if type == 2 {
//            _value_number = UnsafeMutablePointer<OCINumber>.allocate(capacity: 23)
//            status = OCIDefineByPos( stmt, &define, OpaquePointer(err), index, &_value_number, 23, type, nil, &len, &rcode, ub4(OCI_DEFAULT) )
//             LogStatus(status, err)
//
//        }
        
    }
    
    func convertValue() -> Any? {
                    
        switch (type) {
        case 1, 5, 6, 9:
//            _value!.baseAddress![Int(len)] = 0
            return String(cString: _value!.baseAddress!)
        case 2, 4:
            let v = String(cString: _value!.baseAddress!)
            return MIOCoreDecimalValue(v, nil)
        case 3, 8:
            let v = String(cString: _value!.baseAddress!)
            return MIOCoreIntValue(v, nil)
        case 12:
            let v = String(cString: _value!.baseAddress!)
            return MIOCoreDate(fromString: v)

        default: return nil
        }

    }
}


func LogStatus(_ status:Int32, _ err:UnsafeMutableRawPointer?, _ stage:String) {
    var statusMessage = "Unknown"
    var errcode = sb4(0)
    switch(status){
        case OCI_SUCCESS:           statusMessage = "\(stage): SUCCESS"
        case OCI_SUCCESS_WITH_INFO: statusMessage = "\(stage): Error - OCI_SUCCESS_WITH_INFO"
        case OCI_NEED_DATA:         statusMessage = "\(stage): Error - OCI_NEED_DATA"
        case OCI_NO_DATA:           statusMessage = "\(stage): Error - OCI_NODATA"
        case OCI_INVALID_HANDLE:    statusMessage = "\(stage): Error - OCI_INVALID_HANDLE"
        case OCI_STILL_EXECUTING:   statusMessage = "\(stage): Error - OCI_STILL_EXECUTE"
        case OCI_CONTINUE:          statusMessage = "\(stage): Error - OCI_CONTINUE"
        case OCI_ERROR:
            let errbuf = UnsafeMutableBufferPointer<UInt8>.allocate(capacity: 512)
            OCIErrorGet(err, ub4(1), nil, &errcode, errbuf.baseAddress!, ub4(512), ub4(OCI_HTYPE_ERROR))
            statusMessage = String(data: Data(errbuf), encoding: .utf8 ) ?? "Unknown"
        default: break
    }

    print("[MIODBOracleSQLError] Status message: \(stage) \(errcode) \(statusMessage)")
}


