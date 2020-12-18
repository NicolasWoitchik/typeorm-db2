"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var tslib_1 = require("tslib");
var ConnectionIsNotSetError_1 = require("../../error/ConnectionIsNotSetError");
var DriverPackageNotInstalledError_1 = require("../../error/DriverPackageNotInstalledError");
var DB2QueryRunner_1 = require("./DB2QueryRunner");
var DateUtils_1 = require("../../util/DateUtils");
var PlatformTools_1 = require("../../platform/PlatformTools");
var RdbmsSchemaBuilder_1 = require("../../schema-builder/RdbmsSchemaBuilder");
var DriverUtils_1 = require("../DriverUtils");
var OrmUtils_1 = require("../../util/OrmUtils");
var ApplyValueTransformers_1 = require("../../util/ApplyValueTransformers");
/**
 * Organizes communication with IBM DB2 RDBMS.
 */
var DB2Driver = /** @class */ (function () {
    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------
    function DB2Driver(connection) {
        /**
         * Pool for slave databases.
         * Used in replication.
         */
        this.slaves = [];
        /**
         * Indicates if replication is enabled.
         */
        this.isReplicated = false;
        /**
         * Indicates if tree tables are supported by this driver.
         */
        this.treeSupport = true;
        /**
         * Gets list of supported column data types by a driver.
         *
         * @see https://www.ibm.com/support/knowledgecenter/SSEPEK_10.0.0/sqlref/src/tpc/db2z_datatypesintro.html
         */
        this.supportedDataTypes = [
            "smallint",
            "integer",
            "bigint",
            "real",
            "double",
            "float",
            "decimal",
            "numeric",
            "decfloat",
            "varchar",
            "clob",
            "char",
            "varbinary",
            "blob",
            "json",
            "binary",
            "dbclob",
            "date",
            "time",
            "timestamp",
            "timestamp with time zone",
            "timestamp without time zone",
            "boolean",
        ];
        /**
         * Gets list of spatial column data types.
         */
        this.spatialTypes = [];
        /**
         * Gets list of column data types that support length by a driver.
         */
        this.withLengthColumnTypes = ["char", "varchar"];
        /**
         * Gets list of column data types that support precision by a driver.
         */
        this.withPrecisionColumnTypes = [
            "numeric",
            "decfloat",
            "real",
            "number",
            "float",
            "timestamp",
            "timestamp with time zone",
            "timestamp with local time zone",
        ];
        /**
         * Gets list of column data types that support scale by a driver.
         */
        this.withScaleColumnTypes = ["number"];
        /**
         * Orm has special columns and we need to know what database column types should be for those types.
         * Column types are driver dependant.
         */
        this.mappedDataTypes = {
            createDate: "timestamp",
            createDateDefault: "NOW()",
            updateDate: "timestamp",
            updateDateDefault: "NOW()",
            deleteDate: "timestamp",
            deleteDateNullable: true,
            version: "number",
            treeLevel: "number",
            migrationId: "number",
            migrationName: "varchar",
            migrationTimestamp: "number",
            cacheId: "number",
            cacheIdentifier: "varchar",
            cacheTime: "number",
            cacheDuration: "number",
            cacheQuery: "clob",
            cacheResult: "clob",
            metadataType: "varchar",
            metadataDatabase: "varchar",
            metadataSchema: "varchar",
            metadataTable: "varchar",
            metadataName: "varchar",
            metadataValue: "clob",
        };
        /**
         * Default values of length, precision and scale depends on column data type.
         * Used in the cases when length/precision/scale is not specified by user.
         */
        this.dataTypeDefaults = {
            char: { length: 1 },
            varchar: { length: 255 },
            float: { precision: 126 },
            timestamp: { precision: 6 },
            "timestamp with time zone": { precision: 6 },
            "timestamp with local time zone": { precision: 6 },
        };
        /**
         * Max length allowed by IBM for aliases.
         * @see https://www.ibm.com/support/knowledgecenter/SSEPEK_10.0.0/intro/src/tpc/db2z_aliases.html
         *
         */
        this.maxAliasLength = 128;
        this.connection = connection;
        this.options = connection.options;
        // load DB2 package
        this.loadDependencies();
        // // extra DB2 setup
        // this.db2.outFormat = this.db2.OBJECT;
        // Object.assign(connection.options, DriverUtils.buildDriverOptions(connection.options)); // todo: do it better way
        // validate options to make sure everything is set
        // if (!this.options.host)
        //     throw new DriverOptionNotSetError("host");
        // if (!this.options.username)
        //     throw new DriverOptionNotSetError("username");
        // if (!this.options.sid)
        //     throw new DriverOptionNotSetError("sid");
        //
    }
    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------
    /**
     * Performs connection to the database.
     * Based on pooling options, it can either create connection immediately,
     * either create a pool and create connection when needed.
     */
    DB2Driver.prototype.connect = function () {
        return tslib_1.__awaiter(this, void 0, void 0, function () {
            var _a, _b, _c;
            var _this = this;
            return tslib_1.__generator(this, function (_d) {
                switch (_d.label) {
                    case 0:
                        if (!this.options.replication) return [3 /*break*/, 3];
                        _a = this;
                        return [4 /*yield*/, Promise.all(this.options.replication.slaves.map(function (slave) {
                                return _this.createPool(_this.options, slave);
                            }))];
                    case 1:
                        _a.slaves = _d.sent();
                        _b = this;
                        return [4 /*yield*/, this.createPool(this.options, this.options.replication.master)];
                    case 2:
                        _b.master = _d.sent();
                        this.database = this.options.replication.master.database;
                        return [3 /*break*/, 5];
                    case 3:
                        _c = this;
                        return [4 /*yield*/, this.createPool(this.options, this.options)];
                    case 4:
                        _c.master = _d.sent();
                        this.database = this.options.database;
                        _d.label = 5;
                    case 5: return [2 /*return*/];
                }
            });
        });
    };
    /**
     * Makes any action after connection (e.g. create extensions in Postgres driver).
     */
    DB2Driver.prototype.afterConnect = function () {
        return Promise.resolve();
    };
    /**
     * Closes connection with the database.
     */
    DB2Driver.prototype.disconnect = function () {
        return tslib_1.__awaiter(this, void 0, void 0, function () {
            var _this = this;
            return tslib_1.__generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!this.master)
                            return [2 /*return*/, Promise.reject(new ConnectionIsNotSetError_1.ConnectionIsNotSetError("DB2"))];
                        return [4 /*yield*/, this.closePool(this.master)];
                    case 1:
                        _a.sent();
                        return [4 /*yield*/, Promise.all(this.slaves.map(function (slave) { return _this.closePool(slave); }))];
                    case 2:
                        _a.sent();
                        this.master = undefined;
                        this.slaves = [];
                        return [2 /*return*/];
                }
            });
        });
    };
    /**
     * Creates a schema builder used to build and sync a schema.
     */
    DB2Driver.prototype.createSchemaBuilder = function () {
        return new RdbmsSchemaBuilder_1.RdbmsSchemaBuilder(this.connection);
    };
    /**
     * Creates a query runner used to execute database queries.
     */
    DB2Driver.prototype.createQueryRunner = function (mode) {
        return new DB2QueryRunner_1.DB2QueryRunner(this, mode);
    };
    /**
     * Replaces parameters in the given sql with special escaping character
     * and an array of parameter names to be passed to a query.
     */
    DB2Driver.prototype.escapeQueryWithParameters = function (sql, parameters, nativeParameters) {
        var escapedParameters = Object.keys(nativeParameters).map(function (key) {
            if (typeof nativeParameters[key] === "boolean")
                return nativeParameters[key] ? 1 : 0;
            return nativeParameters[key];
        });
        if (!parameters || !Object.keys(parameters).length)
            return [sql, escapedParameters];
        var keys = Object.keys(parameters)
            .map(function (parameter) { return "(:(\\.\\.\\.)?" + parameter + "\\b)"; })
            .join("|");
        sql = sql.replace(new RegExp(keys, "g"), function (key) {
            var value;
            var isArray = false;
            if (key.substr(0, 4) === ":...") {
                isArray = true;
                value = parameters[key.substr(4)];
            }
            else {
                value = parameters[key.substr(1)];
            }
            if (isArray) {
                return value
                    .map(function (v, index) {
                    escapedParameters.push(v);
                    return ":" + key.substr(4) + index;
                })
                    .join(", ");
            }
            else if (value instanceof Function) {
                return value();
            }
            else if (typeof value === "boolean") {
                return value ? 1 : 0;
            }
            else {
                escapedParameters.push(value);
                return key;
            }
        }); // todo: make replace only in value statements, otherwise problems
        return [sql, escapedParameters];
    };
    /**
     * Escapes a column name.
     */
    DB2Driver.prototype.escape = function (columnName) {
        return "\"" + columnName + "\"";
    };
    /**
     * Build full table name with database name, schema name and table name.
     * E.g. "mySchema"."myTable"
     */
    DB2Driver.prototype.buildTableName = function (tableName, schema) {
        return schema ? "\"" + schema + "\".\"" + tableName + "\"" : tableName;
    };
    /**
     * Prepares given value to a value to be persisted, based on its column type and metadata.
     */
    DB2Driver.prototype.preparePersistentValue = function (value, columnMetadata) {
        if (columnMetadata.transformer)
            value = ApplyValueTransformers_1.ApplyValueTransformers.transformTo(columnMetadata.transformer, value);
        if (value === null || value === undefined)
            return value;
        if (columnMetadata.type === Boolean) {
            return value ? 1 : 0;
        }
        else if (columnMetadata.type === "date") {
            if (typeof value === "string")
                value = value.replace(/[^0-9-]/g, "");
            return function () {
                return "TO_DATE('" + DateUtils_1.DateUtils.mixedDateToDateString(value) + "', 'YYYY-MM-DD')";
            };
        }
        else if (columnMetadata.type === Date ||
            columnMetadata.type === "timestamp" ||
            columnMetadata.type === "timestamp with time zone" ||
            columnMetadata.type === "timestamp with local time zone") {
            return DateUtils_1.DateUtils.mixedDateToDate(value);
        }
        else if (columnMetadata.type === "simple-array") {
            return DateUtils_1.DateUtils.simpleArrayToString(value);
        }
        else if (columnMetadata.type === "simple-json") {
            return DateUtils_1.DateUtils.simpleJsonToString(value);
        }
        return value;
    };
    /**
     * Prepares given value to a value to be persisted, based on its column type or metadata.
     */
    DB2Driver.prototype.prepareHydratedValue = function (value, columnMetadata) {
        if (value === null || value === undefined)
            return columnMetadata.transformer
                ? ApplyValueTransformers_1.ApplyValueTransformers.transformFrom(columnMetadata.transformer, value)
                : value;
        if (columnMetadata.type === Boolean) {
            value = !!value;
        }
        else if (columnMetadata.type === "date") {
            value = DateUtils_1.DateUtils.mixedDateToDateString(value);
        }
        else if (columnMetadata.type === "time") {
            value = DateUtils_1.DateUtils.mixedTimeToString(value);
        }
        else if (columnMetadata.type === Date ||
            columnMetadata.type === "timestamp" ||
            columnMetadata.type === "timestamp with time zone" ||
            columnMetadata.type === "timestamp with local time zone") {
            value = DateUtils_1.DateUtils.normalizeHydratedDate(value);
        }
        else if (columnMetadata.type === "json") {
            value = JSON.parse(value);
        }
        else if (columnMetadata.type === "simple-array") {
            value = DateUtils_1.DateUtils.stringToSimpleArray(value);
        }
        else if (columnMetadata.type === "simple-json") {
            value = DateUtils_1.DateUtils.stringToSimpleJson(value);
        }
        if (columnMetadata.transformer)
            value = ApplyValueTransformers_1.ApplyValueTransformers.transformFrom(columnMetadata.transformer, value);
        return value;
    };
    /**
     * Creates a database type from a given column metadata.
     */
    DB2Driver.prototype.normalizeType = function (column) {
        if (column.type === Number ||
            column.type === Boolean ||
            column.type === "numeric" ||
            column.type === "dec" ||
            column.type === "decimal" ||
            column.type === "int" ||
            column.type === "integer" ||
            column.type === "smallint") {
            return "integer";
        }
        else if (column.type === "real" ||
            column.type === "double precision") {
            return "float";
        }
        else if (column.type === String || column.type === "varchar") {
            return "varchar";
        }
        else if (column.type === Date) {
            return "timestamp";
        }
        else if (column.type === Buffer) {
            return "blob";
        }
        else if (column.type === "uuid") {
            return "varchar";
        }
        else if (column.type === "simple-array") {
            return "clob";
        }
        else if (column.type === "simple-json") {
            return "clob";
        }
        else if (column.type === Object) {
            return "blob";
        }
        else {
            return column.type || "";
        }
    };
    /**
     * Normalizes "default" value of the column.
     */
    DB2Driver.prototype.normalizeDefault = function (columnMetadata) {
        var defaultValue = columnMetadata.default;
        if (typeof defaultValue === "number") {
            return "" + defaultValue;
        }
        else if (typeof defaultValue === "boolean") {
            return defaultValue === true ? "1" : "0";
        }
        else if (typeof defaultValue === "function") {
            return defaultValue();
        }
        else if (typeof defaultValue === "string") {
            return "'" + defaultValue + "'";
        }
        else {
            return defaultValue;
        }
    };
    /**
     * Normalizes "isUnique" value of the column.
     */
    DB2Driver.prototype.normalizeIsUnique = function (column) {
        return column.entityMetadata.uniques.some(function (uq) { return uq.columns.length === 1 && uq.columns[0] === column; });
    };
    /**
     * Calculates column length taking into account the default length values.
     */
    DB2Driver.prototype.getColumnLength = function (column) {
        if (column.length)
            return column.length.toString();
        switch (column.type) {
            case String:
            case "varchar":
                return "255";
            case "raw":
                return "2000";
            case "uuid":
                return "36";
            default:
                return "";
        }
    };
    DB2Driver.prototype.createFullType = function (column) {
        var type = column.type;
        // used 'getColumnLength()' method, because in Oracle column length is required for some data types.
        if (this.getColumnLength(column)) {
            type += "(" + this.getColumnLength(column) + ")";
        }
        else if (column.precision !== null &&
            column.precision !== undefined &&
            column.scale !== null &&
            column.scale !== undefined) {
            type += "(" + column.precision + "," + column.scale + ")";
        }
        else if (column.precision !== null &&
            column.precision !== undefined) {
            type += "(" + column.precision + ")";
        }
        if (column.type === "timestamp with time zone") {
            type =
                "TIMESTAMP" +
                    (column.precision !== null && column.precision !== undefined
                        ? "(" + column.precision + ")"
                        : "") +
                    " WITH TIME ZONE";
        }
        else if (column.type === "timestamp with local time zone") {
            type =
                "TIMESTAMP" +
                    (column.precision !== null && column.precision !== undefined
                        ? "(" + column.precision + ")"
                        : "") +
                    " WITH LOCAL TIME ZONE";
        }
        if (column.isArray)
            type += " array";
        return type;
    };
    /**
     * Obtains a new database connection to a master server.
     * Used for replication.
     * If replication is not setup then returns default connection's database connection.
     */
    DB2Driver.prototype.obtainMasterConnection = function () {
        var _this = this;
        return new Promise(function (ok, fail) {
            var credentials = Object.assign({}, _this.options, DriverUtils_1.DriverUtils.buildDriverOptions(_this.options)); // todo: do it better way
            // build connection options for the driver
            var connectionOptions = Object.assign({}, {
                connectString: credentials.connectString
                    ? credentials.connectString
                    : "DATABASE=" + credentials.database + ";HOSTNAME=" + credentials.host + ";PORT=" + credentials.port + ";PROTOCOL=TCPIP;UID=" + credentials.username + ";PWD=" + credentials.password + ";Security=SSL",
            }, _this.options.extra || {});
            _this.master.open(connectionOptions.connectString, function (err, connection) {
                if (err)
                    return fail(err);
                ok(connection);
            });
        });
    };
    /**
     * Obtains a new database connection to a slave server.
     * Used for replication.
     * If replication is not setup then returns master (default) connection's database connection.
     */
    DB2Driver.prototype.obtainSlaveConnection = function () {
        var _this = this;
        if (!this.slaves.length)
            return this.obtainMasterConnection();
        return new Promise(function (ok, fail) {
            var random = Math.floor(Math.random() * _this.slaves.length);
            var credentials = Object.assign({}, _this.options, DriverUtils_1.DriverUtils.buildDriverOptions(_this.options)); // todo: do it better way
            // build connection options for the driver
            var connectionOptions = Object.assign({}, {
                connectString: credentials.connectString
                    ? credentials.connectString
                    : "DATABASE=" + credentials.database + ";HOSTNAME=" + credentials.host + ";PORT=" + credentials.port + ";PROTOCOL=TCPIP;UID=" + credentials.username + ";PWD=" + credentials.password + ";Security=SSL",
            }, _this.options.extra || {});
            _this.slaves[random].open(connectionOptions.connectString, function (err, connection) {
                if (err)
                    return fail(err);
                ok(connection);
            });
        });
    };
    /**
     * Creates generated map of values generated or returned by database after INSERT query.
     */
    DB2Driver.prototype.createGeneratedMap = function (metadata, insertResult) {
        var _this = this;
        if (!insertResult)
            return undefined;
        return Object.keys(insertResult).reduce(function (map, key) {
            var column = metadata.findColumnWithDatabaseName(key);
            if (column) {
                OrmUtils_1.OrmUtils.mergeDeep(map, column.createValueMap(_this.prepareHydratedValue(insertResult[key], column)));
            }
            return map;
        }, {});
    };
    /**
     * Differentiate columns of this table and columns from the given column metadatas columns
     * and returns only changed.
     */
    DB2Driver.prototype.findChangedColumns = function (tableColumns, columnMetadatas) {
        var _this = this;
        return columnMetadatas.filter(function (columnMetadata) {
            var tableColumn = tableColumns.find(function (c) { return c.name === columnMetadata.databaseName; });
            if (!tableColumn)
                return false; // we don't need new columns, we only need exist and changed
            return (tableColumn.name !== columnMetadata.databaseName ||
                tableColumn.type !== _this.normalizeType(columnMetadata) ||
                tableColumn.length !== columnMetadata.length ||
                tableColumn.precision !== columnMetadata.precision ||
                tableColumn.scale !== columnMetadata.scale ||
                // || tableColumn.comment !== columnMetadata.comment || // todo
                _this.normalizeDefault(columnMetadata) !== tableColumn.default ||
                tableColumn.isPrimary !== columnMetadata.isPrimary ||
                tableColumn.isNullable !== columnMetadata.isNullable ||
                tableColumn.isUnique !==
                    _this.normalizeIsUnique(columnMetadata) ||
                (columnMetadata.generationStrategy !== "uuid" &&
                    tableColumn.isGenerated !== columnMetadata.isGenerated));
        });
    };
    /**
     * Returns true if driver supports RETURNING / OUTPUT statement.
     */
    DB2Driver.prototype.isReturningSqlSupported = function () {
        return true;
    };
    /**
     * Returns true if driver supports uuid values generation on its own.
     */
    DB2Driver.prototype.isUUIDGenerationSupported = function () {
        return false;
    };
    /**
     * Returns true if driver supports fulltext indices.
     */
    DB2Driver.prototype.isFullTextColumnTypeSupported = function () {
        return false;
    };
    /**
     * Creates an escaped parameter.
     */
    DB2Driver.prototype.createParameter = function (parameterName, index) {
        return "?";
        // return ":" + (index + 1);
    };
    // /**
    //  * Converts column type in to native oracle type.
    //  */
    // columnTypeToNativeParameter(type: ColumnType): any {
    //     switch (this.normalizeType({ type: type as any })) {
    //         case "number":
    //         case "numeric":
    //         case "int":
    //         case "integer":
    //         case "smallint":
    //         case "dec":
    //         case "decimal":
    //             return this.db2.NUMBER;
    //         case "char":
    //         case "nchar":
    //         case "nvarchar2":
    //         case "varchar2":
    //             return this.db2.STRING;
    //         case "blob":
    //             return this.db2.BLOB;
    //         case "clob":
    //             return this.db2.CLOB;
    //         case "date":
    //         case "timestamp":
    //         case "timestamp with time zone":
    //         case "timestamp with local time zone":
    //             return this.db2.DATE;
    //     }
    // }
    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------
    /**
     * Loads all driver dependencies.
     */
    DB2Driver.prototype.loadDependencies = function () {
        try {
            this.db2 = PlatformTools_1.PlatformTools.load("ibm_db");
            if (this.options.debug) {
                this.db2.debug(this.options.debug);
            }
        }
        catch (e) {
            throw new DriverPackageNotInstalledError_1.DriverPackageNotInstalledError("DB2", "ibm_db");
        }
    };
    /**
     * Creates a new connection pool for a given database credentials.
     */
    DB2Driver.prototype.createPool = function (options, credentials) {
        return tslib_1.__awaiter(this, void 0, void 0, function () {
            var connectionOptions, pool;
            return tslib_1.__generator(this, function (_a) {
                credentials = Object.assign({}, credentials, DriverUtils_1.DriverUtils.buildDriverOptions(credentials)); // todo: do it better way
                connectionOptions = Object.assign({}, {
                    connectString: credentials.connectString
                        ? credentials.connectString
                        : "DATABASE=" + credentials.database + ";HOSTNAME=" + credentials.host + ";PORT=" + credentials.port + ";PROTOCOL=TCPIP;UID=" + credentials.username + ";PWD=" + credentials.password + ";Security=SSL",
                }, options.extra || {});
                pool = new this.db2.Pool(connectionOptions.connectionsString);
                // pooling is enabled either when its set explicitly to true,
                // either when its not defined at all (e.g. enabled by default)
                return [2 /*return*/, new Promise(function (ok, fail) {
                        var opened = pool.init(5, connectionOptions.connectionsString);
                        if (!opened)
                            return fail("Failed to open pool");
                        ok(pool);
                        // pool.open(connectionOptions, (err: any, pool: any) => {
                        //     if (err) return fail(err);
                        //     ok(pool);
                        // });
                    })];
            });
        });
    };
    /**
     * Closes connection pool.
     */
    DB2Driver.prototype.closePool = function (pool) {
        return tslib_1.__awaiter(this, void 0, void 0, function () {
            return tslib_1.__generator(this, function (_a) {
                return [2 /*return*/, new Promise(function (ok, fail) {
                        pool.close(function (err) { return (err ? fail(err) : ok()); });
                        pool = undefined;
                    })];
            });
        });
    };
    return DB2Driver;
}());
exports.DB2Driver = DB2Driver;

//# sourceMappingURL=DB2Driver.js.map
