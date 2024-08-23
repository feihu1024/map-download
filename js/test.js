const utils = require('./utils');
const sqlite3 = require('sqlite3').verbose();
function getGoogleTilesByLevel(startTile, size, level, bounds) {
    const { minX, minY, maxX, maxY, rowCount } = bounds;
    const tileList = [];

    // google瓦片方案下反转x和y
    const startY = minX;
    const startX = minY;
    const endX = maxY;
    const endY = maxX;

    let x;
    let y;

    // 根据瓦片编号计算偏移
    const offset = startTile ? (startTile.x - minY) * rowCount + (startTile.y - minX) + 1 : 0;

    // 根据偏移计算瓦片编号
    for (let i = 0; i < size; i++) {
        y = startY + ((i + offset) % rowCount);
        x = startX + Math.floor((i + offset) / rowCount);
        if (x > endX || y > endY) break;
        tileList.push({ level, y, x });
    }
    return tileList;
}

function test(size) {
    const configPath = './config/task-config.json';
    let config = utils.readJson(configPath);
    const task = config.taskList.find((item) => item.id === '1723002553436');

    const minzoom = 12;
    const maxzoom = 14;

    const [minX, minY, maxX, maxY] = task.metadata.bounds;
    const bounds = { minX, minY, maxX, maxY };

    for (let index = minzoom; index <= maxzoom; index++) {
        const range = utils.bounds2Tile(bounds, index);

        const listSize = size;
        const rowCount = range.maxX - range.minX + 1;
        const colCount = range.maxY - range.minY + 1;

        let tileList;
        let startTile = null;
        let xx = 1;
        while (true) {
            // 获取瓦片分组
            tileList = getGoogleTilesByLevel(startTile, listSize, index, { ...range, rowCount, colCount });

            // 如果获取瓦片数量为0，则说明当前level下的所有瓦片都已经下载完成
            if (tileList.length < 1) break;

            console.log(`level:${index} count: ${rowCount * colCount}_${xx} offset:${JSON.stringify(startTile)}----->>>>`, tileList.map((item) => `${item.level}-${item.y}-${item.x}`).join(', '));
            xx++;
            // 当前分组下载成功后，则更新起始瓦片，以便获取下一个分组
            startTile = tileList[tileList.length - 1];
        }
    }
}

// test(12);

// 全球范围：-180,85.0511,179.999999,-85.0511
// 中国范围：72.86133,53.80065,145.81055,1.14250
// 云贵川渝：96.76758,34.37971,110.30273,20.96144

function testRate(arr) {
    const time = Math.floor((new Date(arr[2]).getTime() - new Date(arr[0]).getTime()) / 1000);
    const count = arr[3] - arr[1];
    const rateStr = Math.floor(count / time) + '/s';
    return `时长：${time}s 数量：${count} 速度：${rateStr}`;
}
// testRate(['2024-08-21 10:37:20', 2717700, '2024-08-21 10:38:45', 2736938]);

const sqlRunHelper = (db, cmd, ...args) => {
    return new Promise((resolve, reject) => {
        db[cmd](...args, function (err) {
            err ? reject(err) : resolve(this);
        });
    });
};

const sqlQueryHelper = (db, cmd, ...args) => {
    return new Promise((resolve, reject) => {
        db[cmd](...args, function (err, data) {
            err ? reject(err) : resolve(data);
        });
    });
};

// 全球范围
// 1 {minX: 0, minY: 0, maxX: 1, maxY: 1} row:2: col:2 tiles:4
// 2 {minX: 0, minY: 0, maxX: 3, maxY: 3} row:4: col:4 tiles:16
// 3 {minX: 0, minY: 0, maxX: 7, maxY: 7} row:8: col:8 tiles:64
// 4 {minX: 0, minY: 0, maxX: 15, maxY: 15} row:16: col:16 tiles:256
// 5 {minX: 0, minY: 0, maxX: 31, maxY: 31} row:32: col:32 tiles:1024
// 6 {minX: 0, minY: 0, maxX: 63, maxY: 63} row:64: col:64 tiles:4096
// 7 {minX: 0, minY: 0, maxX: 127, maxY: 127} row:128: col:128 tiles:16384
// 8 {minX: 0, minY: 0, maxX: 255, maxY: 255} row:256: col:256 tiles:65536
// 全国范围
// 9 {minX: 359, minY: 164, maxX: 463, maxY: 254} row:105: col:91 tiles:9555
// 10 {minX: 719, minY: 329, maxX: 926, maxY: 508} row:208: col:180 tiles:37440
// 11 {minX: 1438, minY: 659, maxX: 1853, maxY: 1017} row:416: col:359 tiles:149344
// 12 {minX: 2877, minY: 1319, maxX: 3707, maxY: 2035} row:831: col:717 tiles:595827
// 13 {minX: 5754, minY: 2638, maxX: 7414, maxY: 4070} row:1661: col:1433 tiles:2380213
// 14 {minX: 11508, minY: 5276, maxX: 14828, maxY: 8140} row:3321: col:2865 tiles:9514665
// 云贵川渝范围
// 15 {minX: 25192, minY: 13048, maxX: 26423, maxY: 14431} row:1232: col:1384 tiles:1705088
// 16 {minX: 50384, minY: 26096, maxX: 52847, maxY: 28863} row:2464: col:2768 tiles:6820352
// 17 {minX: 100768, minY: 52192, maxX: 105695, maxY: 57727} row:4928: col:5536 tiles:27281408
// 18 {minX: 201536, minY: 104384, maxX: 211391, maxY: 115455} row:9856: col:11072 tiles:109125632

testSql();
async function testSql() {
    const configPath = './config/task_global_vec.json';
    const db = new sqlite3.Database('files/tdt_vec_1-17.mbtiles');
    let config = utils.readJson(configPath);
    const task = config.taskList.find((item) => item.id === '1724202471924');

    const minzoom = task.metadata.minzoom;
    const maxzoom = task.metadata.maxzoom;

    const [minX, minY, maxX, maxY] = task.metadata.bounds;
    const bounds = { minX, minY, maxX, maxY };

    for (let index = minzoom; index <= maxzoom; index++) {
        const range = utils.bounds2Tile(bounds, index);

        const rowCount = range.maxX - range.minX + 1;
        const colCount = range.maxY - range.minY + 1;
        const tileCount = rowCount * colCount;
        const result = await sqlQueryHelper(db, 'get', `select datetime(CURRENT_TIMESTAMP,'localtime') as time, count(zoom_level) as total from tiles where zoom_level=?;`, [index]);

        if (result.total === tileCount) {
            console.log(`${index}级 目标数量：${tileCount} 当前数量：${result.total} 结果一致 `);
        } else {
            console.log(`${index}级 目标数量：${tileCount} 当前数量：${result.total} 结果不一致 `);
            let errColumnCount = 0;
            for (let column = range.minY; column <= range.maxY; column++) {
                const columnResult = await sqlQueryHelper(db, 'get', `select count(zoom_level) as total from tiles where zoom_level=? and tile_row=?;`, [index, column]);
                if (columnResult.total !== rowCount) {
                    console.log(`${index}级-->>${column}列 目标数量：${rowCount} 当前数量：${columnResult.total} 结果不一致 `);
                    errColumnCount++;
                }
            }
            console.log('缺失行数：', errColumnCount, '行');
            if (errRowCount > 0) break;
        }
    }
}
