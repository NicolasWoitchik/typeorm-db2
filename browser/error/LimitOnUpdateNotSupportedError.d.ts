/**
 * Thrown when user tries to build an UPDATE query with LIMIT but the database does not support it.
*/
export declare class LimitOnUpdateNotSupportedError extends Error {
    name: string;
    constructor();
}
