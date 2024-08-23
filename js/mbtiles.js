const sqlite3 = require('sqlite3').verbose();

const defaultFormater = (value) => value;
const defaultGetValue = (value) => value;

const sqlRunHelper = (db, ...args) => {
    return new Promise((resolve, reject) => {
        db.run(...args, function (err) {
            err ? reject(err) : resolve(this);
        });
    });
};

const sqlFinalizeHelper = (db) => {
    return new Promise((resolve, reject) => {
        db.finalize(function (err) {
            err ? reject(err) : resolve(this);
        });
    });
};

class Metadata {
    static COLUMNS = [
        { name: 'version', formater: defaultFormater, getValue: defaultGetValue },
        { name: 'type', formater: defaultFormater, getValue: defaultGetValue },
        { name: 'format', formater: defaultFormater, getValue: defaultGetValue },
        { name: 'minzoom', formater: defaultFormater, getValue: defaultGetValue },
        { name: 'maxzoom', formater: defaultFormater, getValue: defaultGetValue },
        { name: 'center', formater: (value) => value.join(), getValue: (value) => value.split(',').map((v) => Number(v)) },
        { name: 'bounds', formater: (value) => value.join(), getValue: (value) => value.split(',').map((v) => Number(v)) },
        { name: 'name', formater: defaultFormater, getValue: defaultGetValue },
        { name: 'attribution', formater: defaultFormater, getValue: defaultGetValue },
        { name: 'description', formater: defaultFormater, getValue: defaultGetValue },
        { name: 'url', formater: defaultFormater, getValue: defaultGetValue }
    ];
    static SQL_CREATE_METADATA = `CREATE TABLE IF NOT EXISTS metadata (name TEXT,value TEXT);`;
    static SQL_INSERTS_METADATA = `INSERT INTO metadata (name,value) VALUES (?,?);`;
    static SQL_UPDATE_METADATA = `UPDATE metadata SET value = ? WHERE name = ?;`;
    db = null;
    constructor(path, option) {
        this.db = new sqlite3.Database(path, (err) => {
            console.log(err);
        });
        if (option) {
            this.init(option);
        }
    }
    async init(option) {
        // 删除旧表
        await sqlRunHelper(this.db, 'DROP TABLE IF EXISTS metadata;');

        // 创建新表
        await sqlRunHelper(this.db, Metadata.SQL_CREATE_METADATA);

        for (const key in option) {
            const column = Metadata.COLUMNS.find((item) => item.name === key);
            if (column) {
                const value = column.formater(option[column.name]);
                await sqlRunHelper(this.db, Metadata.SQL_INSERTS_METADATA, [column.name, value]);
            }
        }
    }
    async update(option) {
        for (const key in option) {
            const column = Metadata.COLUMNS.find((item) => item.name === key);
            if (column) {
                const value = column.formater(option[column.name]);
                await sqlRunHelper(this.db, Metadata.SQL_UPDATE_METADATA, [value, column.name]);
            }
        }
    }
    async getInfo(column) {
        return new Promise((resolve, reject) => {
            if (column) {
                this.db.get('SELECT * FROM metadata where name = ?;', [column], (err, row) => {
                    if (err) {
                        return reject(err);
                    }
                    const result = {};
                    const column = Metadata.COLUMNS.find((item) => item.name === row.name);
                    if (column) result[row.name] = column.getValue(row.value);
                    resolve(result);
                });
            } else {
                this.db.all('SELECT * FROM metadata;', (err, rows) => {
                    if (err) {
                        return reject(err);
                    }
                    const result = {};
                    rows.forEach((row) => {
                        const column = Metadata.COLUMNS.find((item) => item.name === row.name);
                        if (column) result[row.name] = column.getValue(row.value);
                    });
                    resolve(result);
                });
            }
        });
    }
    close() {
        return new Promise((resolve, reject) => {
            this.db.close((err) => {
                if (err) reject(err);
                resolve();
            });
        });
    }
}

class Tiles {
    static SQL_DROP_TILES = `DROP TABLE IF EXISTS tiles;`;
    static SQL_CREATE_TILES = `CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);`;
    static SQL_CREATE_TILES_INDEX = `CREATE UNIQUE INDEX tile_index on tiles (zoom_level, tile_column, tile_row);`;
    static SQL_INSERT_TILES = `INSERT INTO tiles (zoom_level, tile_row, tile_column, tile_data) VALUES (?, ?, ?, ?);`; // google方案需要把xy进行反转
    static SQL_UPDATE_TILES = `UPDATE tiles SET zoom_level = ?,tile_column = ?, tile_row = ?, tile_data = ? WHERE zoom_level = ? AND tile_row = ? AND tile_column = ?;`;
    static SQL_QUERY_TILES = `SELECT zoom_level, tile_row, tile_column FROM tiles WHERE zoom_level = ? AND tile_row = ? AND tile_column = ?;`;
    db = null;
    constructor(path) {
        this.db = new sqlite3.Database(path);
        sqlRunHelper(this.db, `PRAGMA synchronous = OFF`);
        sqlRunHelper(this.db, `PRAGMA journal_mode = WAL;`);
    }
    async init() {
        // 删除旧表
        // await sqlRunHelper(this.db, SQL_DROP_TILES);

        // 创建新表
        await sqlRunHelper(this.db, Tiles.SQL_CREATE_TILES);

        // 创建索引
        await sqlRunHelper(this.db, Tiles.SQL_CREATE_TILES_INDEX);
    }
    async save(z, x, y, tile) {
        await sqlRunHelper(this.db, Tiles.SQL_INSERT_TILES, [z, x, y, tile]);
    }
    async saveList(tileList) {
        return new Promise(async (resolve, reject) => {
            let stmt = null;
            try {
                stmt = this.db.prepare(Tiles.SQL_INSERT_TILES);
                await sqlRunHelper(this.db, 'BEGIN TRANSACTION');
                for (const tile of tileList) {
                    await sqlRunHelper(stmt, tile.level, tile.x, tile.y, tile.data);
                }
                await sqlFinalizeHelper(stmt);
                await sqlRunHelper(this.db, 'COMMIT');
                resolve();
            } catch (err) {
                await sqlRunHelper(this.db, 'ROLLBACK');
                stmt && (await sqlFinalizeHelper(stmt));
                reject(err);
            }
        });
    }
    async update(z, x, y, tile) {
        await sqlRunHelper(this.db, Tiles.SQL_UPDATE_TILES, [z, x, y, tile, z, x, y]);
    }
    has(z, x, y) {
        return new Promise((resolve, reject) => {
            this.db.get(Tiles.SQL_QUERY_TILES, [z, x, y], (err, row) => {
                if (err) {
                    return reject(err);
                }
                return resolve(!!row);
            });
        });
    }
}
module.exports = { Metadata, Tiles };
