import { MissingDriverError } from "../error/MissingDriverError";
import { CockroachDriver } from "./cockroachdb/CockroachDriver";
import { MongoDriver } from "./mongodb/MongoDriver";
import { SqlServerDriver } from "./sqlserver/SqlServerDriver";
import { OracleDriver } from "./oracle/OracleDriver";
import { SqliteDriver } from "./sqlite/SqliteDriver";
import { CordovaDriver } from "./cordova/CordovaDriver";
import { ReactNativeDriver } from "./react-native/ReactNativeDriver";
import { NativescriptDriver } from "./nativescript/NativescriptDriver";
import { SqljsDriver } from "./sqljs/SqljsDriver";
import { MysqlDriver } from "./mysql/MysqlDriver";
import { PostgresDriver } from "./postgres/PostgresDriver";
import { ExpoDriver } from "./expo/ExpoDriver";
import { AuroraDataApiDriver } from "./aurora-data-api/AuroraDataApiDriver";
import { AuroraDataApiPostgresDriver } from "./aurora-data-api-pg/AuroraDataApiPostgresDriver";
import { SapDriver } from "./sap/SapDriver";
import { BetterSqlite3Driver } from "./better-sqlite3/BetterSqlite3Driver";
import { DB2Driver } from "./db2/DB2Driver";
/**
 * Helps to create drivers.
 */
var DriverFactory = /** @class */ (function () {
    function DriverFactory() {
    }
    /**
     * Creates a new driver depend on a given connection's driver type.
     */
    DriverFactory.prototype.create = function (connection) {
        var type = connection.options.type;
        switch (type) {
            case "mysql":
                return new MysqlDriver(connection);
            case "postgres":
                return new PostgresDriver(connection);
            case "cockroachdb":
                return new CockroachDriver(connection);
            case "sap":
                return new SapDriver(connection);
            case "mariadb":
                return new MysqlDriver(connection);
            case "sqlite":
                return new SqliteDriver(connection);
            case "better-sqlite3":
                return new BetterSqlite3Driver(connection);
            case "cordova":
                return new CordovaDriver(connection);
            case "nativescript":
                return new NativescriptDriver(connection);
            case "react-native":
                return new ReactNativeDriver(connection);
            case "sqljs":
                return new SqljsDriver(connection);
            case "oracle":
                return new OracleDriver(connection);
            case "mssql":
                return new SqlServerDriver(connection);
            case "mongodb":
                return new MongoDriver(connection);
            case "expo":
                return new ExpoDriver(connection);
            case "aurora-data-api":
                return new AuroraDataApiDriver(connection);
            case "aurora-data-api-pg":
                return new AuroraDataApiPostgresDriver(connection);
            case "db2":
                return new DB2Driver(connection);
            default:
                throw new MissingDriverError(type);
        }
    };
    return DriverFactory;
}());
export { DriverFactory };

//# sourceMappingURL=DriverFactory.js.map
