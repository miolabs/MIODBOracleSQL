//
//  MDBOracleConnection.swift
//  
//
//  Created by Javier Segura Perez on 30/11/21.
//

import Foundation
import MIODB


open class MDBOracleConnection : MDBConnection
{
    public override func create ( ) throws -> MIODB {
        let db = MIODBOracleSQL( connection: self )
        try db.connect()
        return db
    }
}
