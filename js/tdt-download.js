const axios = require('axios');
const moment = require('moment');
const fs = require('fs');

const utils = require('./utils');
const { Metadata, Tiles } = require('./mbtiles');

const timeFormat = 'YYYY-MM-DD HH:mm:ss';

class Tile {
    static STATUS_SUCCESS = 0;
    static STATUS_EXISTS = 1;
    static STATUS_FAIL = 2;
    static ERROR_UNKNOWN = -1;
    static ERROR_KEY_LIMIT = -2;
    constructor(level = 0, x = 0, y = 0) {
        this.level = level;
        this.x = x;
        this.y = y;
    }
}

class TileDownloadError extends Error {
    name = 'TileDownloadError';
    options;
    constructor(msg = '切片下载错误', options) {
        super(msg);
        this.option = options;
    }
    toString() {
        return super.toString() + JSON.stringify(this.option);
    }
}

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

function requestTile(url) {
    return axios
        .get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Safari/537.36',
                Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate'
            },
            responseType: 'arraybuffer'
        })
        .then((res) => res.data);
}

// 下载一个切片
function downloadTile(tile, option) {
    const { key, mbtiles, tileUrl } = option;
    const { level, x, y } = tile;
    const server = utils.getRandom(0, 8);
    // 构造下载地址url
    const url = tileUrl.replace('{s}', server).replace('{key}', key).replace('{z}', level).replace('{x}', y).replace('{y}', x);
    const tileId = `${level}-${x}-${y}`;
    return new Promise(async (resolve, reject) => {
        let error;
        let errorType;
        for (let i = 0; i < 3; i++) {
            try {
                const tileExists = await mbtiles.has(level, x, y);
                if (!tileExists) {
                    const tileData = await requestTile(url); // 下载切片二进制数据
                    mbtiles.save(level, x, y, tileData);
                    return resolve({ ...tile, status: Tile.STATUS_SUCCESS });
                } else {
                    return resolve({ ...tile, status: Tile.STATUS_EXISTS });
                }
            } catch (err) {
                const { status, statusText, data } = err?.response || {};

                if (data) {
                    let errData = data.toString('utf-8');
                    try {
                        errData = JSON.parse(errData);
                    } catch (err) {}
                    error = new TileDownloadError('切片下载异常', { url, status, statusText, data: errData });
                    if (status === 429) {
                        errorType = Tile.ERROR_KEY_LIMIT;
                    }
                } else {
                    error = new Error('发生未知异常');
                    errorType = Tile.ERROR_KEY_LIMIT;
                }

                // console.log(`${tileId}下载失败，即将开始第${i}次重试.........`);
            }
        }
        const errDetailMsg = `${moment().format(timeFormat)}: \n\t瓦片编号：${tileId}\n\t下载地址：${url}\n\t错误详情：${error}\n`;
        utils.logToFile(`./logs/error-detail.log`, errDetailMsg);
        // const errMsg = `${moment().format(timeFormat)} ${tileId} ${url}\n`;
        // utils.logToFile('./logs/error.log', errMsg);
        reject({ ...tile, status: Tile.STATUS_FAIL, type: errorType });
    });
}

function downloadTileByGroup(tileGroup, option) {
    return new Promise(async (resolve) => {
        let tileList = tileGroup;
        let keyLimitErrorCount = 0;
        while (tileList.length > 0) {
            const pList = tileList.map((tile) => downloadTile(tile, option));
            const list = await Promise.allSettled(pList);
            tileList = [];
            list.forEach((item) => {
                if (item.status === 'rejected' && item.reason.status === Tile.STATUS_FAIL) {
                    const tile = { ...item.reason };
                    delete tile.status;
                    tileList.push(tile);
                    // 记录因为tk达到上限而导致下载失败的数量
                    item.reason.type === Tile.ERROR_KEY_LIMIT && keyLimitErrorCount++;
                }
            });
            // 如果所有的下载错误都是因为tk达到上限，则认为下载失败
            if (keyLimitErrorCount >= tileGroup.length) return resolve({ status: Tile.STATUS_FAIL, type: Tile.ERROR_KEY_LIMIT });

            // 输出提示信息
            if (tileList.length > 0) {
                const tileId = `${tileGroup[0].level}-${tileGroup[0].x}-${tileGroup[0].y}`;
                console.log(`${moment().format(timeFormat)}: ${tileId} 有${tileList.length}个瓦片下载失败，即将开始重试.........`);
            }
        }
        resolve();
    });
}

/** 下载指定层级的所有地图瓦片 */
function downloadByLevel(level, range, configOption) {
    const { currentTile, mbtiles, task, config, configPath } = configOption;
    return new Promise(async (resolve, reject) => {
        const listSize = 256;
        const rowCount = range.maxX - range.minX + 1;
        const colCount = range.maxY - range.minY + 1;
        const keyList = config.keyList;
        const tileCount = rowCount * colCount;

        let tileList;
        let startTile = currentTile;
        let keyIndex = 0;
        const options = { key: keyList[keyIndex], mbtiles, tileUrl: task.metadata.url };

        let successCount = startTile ? (startTile.x - range.minY) * rowCount + (startTile.y - range.minX) + 1 : 0;

        while (true) {
            const t = Date.now();
            // 获取瓦片分组
            tileList = getGoogleTilesByLevel(startTile, listSize, level, { ...range, rowCount, colCount });

            // 如果获取瓦片数量为0，则说明当前level下的所有瓦片都已经下载完成
            if (tileList.length < 1) return resolve();

            // 执行下载
            const err = await downloadTileByGroup(tileList, options);
            successCount += tileList.length;

            const startId = `${level}-${tileList[0].y}-${tileList[0].x}`;
            console.log(`${moment().format(timeFormat)}: ${startId}开始共${tileList.length}个切片下载${err ? '失败' : '成功'},耗时：${Date.now() - t}ms (${((successCount / tileCount).toFixed(4) * 100).toFixed(2)}%: ${successCount}/${tileCount})\n`);

            // 如果失败，则重新下载该组切片
            if (err?.type === Tile.ERROR_KEY_LIMIT) {
                options.key = keyList[++keyIndex];
                if (keyIndex >= keyList.length) {
                    reject(new Error('tk已用完，请更换tk'));
                }
                continue;
            }

            // 当前分组下载成功后，则更新起始瓦片，以便获取下一个分组
            startTile = tileList[tileList.length - 1];

            // 更新当前任务的下载状态，同步到配置文件中
            task.currentGroupId = `${level}-${startTile.y}-${startTile.x}`;
            utils.writeToFile(configPath, JSON.stringify(config, null, 4));
        }
    });
}

async function startTask(task, config, configPath) {
    const [minX, minY, maxX, maxY] = task.metadata.bounds;
    const bounds = { minX, minY, maxX, maxY };

    // 初始化MBTiles数据库
    const mbtiles = new Tiles(task.filePath);
    if (task.dbInit === false) {
        // 初始化metadata数据表
        const dbMetaData = new Metadata(task.filePath);
        await dbMetaData.init(task.metadata);
        await dbMetaData.close();

        // 初始化tile数据表
        await mbtiles.init(task.filePath);

        task.dbInit = true;
        utils.writeToFile(configPath, JSON.stringify(config, null, 4));
    }

    // 获取当前任务的当前瓦片信息
    const [level, y, x] = task?.currentGroupId ? task.currentGroupId.split('-') : [];
    const startLevel = Number(level) || task.metadata.minzoom;
    let startTile = task?.currentGroupId ? { level: Number(level), x: Number(x), y: Number(y) } : null;
    const opt = { mbtiles, task, config, configPath };

    for (let index = startLevel; index <= task.metadata.maxzoom; index++) {
        const t = Date.now();
        const tileRange = utils.bounds2Tile(bounds, index);
        await downloadByLevel(index, tileRange, { currentTile: startTile, ...opt });
        startTile = null;
        console.log(`${moment().format(timeFormat)}: 下载完成：${index}，${JSON.stringify(tileRange)},耗时：${Date.now() - t}ms`);
    }

    // 更新当前任务的下载状态，同步到配置文件中
    delete task.currentGroupId;
    task.complete = true;
    utils.writeToFile(configPath, JSON.stringify(config, null, 4));
}

module.exports = { startTask };
