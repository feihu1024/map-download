const axios = require('axios');
const moment = require('moment');
const fs = require('fs');

const utils = require('./utils');
const { Metadata, Tiles } = require('./mbtiles');

const timeFormat = 'YYYY-MM-DD HH:mm:ss';

const configPath = './config/task-config.json';

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

function getTilesByLevel(startTile, size, level, bounds) {
    const { minX, minY, maxX, maxY } = bounds;
    const tileList = [];
    const startIndex = !startTile ? 0 : 1;
    const tileCount = !startTile ? size : size + 1;
    const limitY = maxY + 1;
    const startX = startTile?.x || minX;
    const startY = startTile?.y || minY;
    let x = startX;
    let y = startY;
    for (let i = startIndex; i < tileCount; i++) {
        x = startX + Math.floor((startY + i) / limitY);
        y = (startY + i) % limitY;
        if (x > maxX) break;
        tileList.push({ level, x, y });
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
    const { key, filePath, tileUrl } = option;
    const { level, x, y } = tile;
    const server = utils.getRandom(0, 8);
    // 构造下载地址url
    const url = tileUrl.replace('{s}', server).replace('{key}', key).replace('{z}', level).replace('{x}', x).replace('{y}', y);
    const tilePath = `${filePath}/${level}/${x}/${y}.png`;
    return new Promise(async (resolve, reject) => {
        let error;
        let errorType;
        for (let i = 0; i < 1; i++) {
            try {
                if (!fs.existsSync(tilePath)) {
                    const tileData = await requestTile(url); // 下载切片二进制数据
                    utils.writeToFile(tilePath, tileData); // 写入文件
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
        const errDetailMsg = `${moment().format(timeFormat)}: \n\t瓦片编号：${tileId}\n\t下载地址：${url}\n\t错误详情：${error}\n\n`;
        utils.logToFile(`./logs/error-detail.log`, errDetailMsg);
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
function downloadByLevel(level, range, groupTile, task, config) {
    const listSize = 64;
    const keyList = config.keyList;

    return new Promise(async (resolve, reject) => {
        let tileList;
        let startTile = groupTile;
        let keyIndex = 0;
        const options = { key: keyList[keyIndex], filePath: task.filePath, tileUrl: task.metadata.url };

        while (true) {
            // 获取瓦片分组
            tileList = getTilesByLevel(startTile, listSize, level, range, options);

            // 如果获取瓦片数量为0，则说明当前level下的所有瓦片都已经下载完成
            if (tileList.length < 1) return resolve();

            // 执行下载
            const err = await downloadTileByGroup(tileList, options);

            const startId = `${level}-${tileList[0].x}-${tileList[0].y}`;
            console.log(`${moment().format(timeFormat)}: ${startId}开始共${tileList.length}个切片下载${err ? '失败' : '成功'}\n`);

            // 如果失败，则重新下载该组切片
            if (err?.type === Tile.ERROR_KEY_LIMIT) {
                options.key = keyList[++keyIndex];
                if (keyIndex >= keyList.length) {
                    reject(new Error('tk已用完，请更换tk'));
                }
                continue;
            }

            // 更新当前任务的下载状态，同步到配置文件中
            task.currentGroupId = startId;
            utils.writeToFile(configPath, JSON.stringify(config, null, 4));

            // 当前分组下载成功后，则更新起始瓦片，以便获取下一个分组
            startTile = tileList[tileList.length - 1];
        }
    });
}

async function startTask(task, config) {
    const [minX, minY, maxX, maxY] = task.metadata.bounds;
    const bounds = { minX, minY, maxX, maxY };

    // 获取当前任务的当前瓦片信息
    const [level, x, y] = task?.currentGroupId ? task.currentGroupId.split('-') : [];
    const startLevel = Number(level) || task.metadata.minzoom;
    const startTile = task?.currentGroupId ? { level: Number(level), x: Number(x), y: Number(y) } : null;

    for (let index = startLevel; index <= task.metadata.maxzoom; index++) {
        const tileRange = utils.bounds2Tile(bounds, index);
        const t = Date.now();
        await downloadByLevel(index, tileRange, startTile, mbtiles, task, config);
        console.log(`${moment().format(timeFormat)}: 下载完成：${index}，${JSON.stringify(tileRange)},耗时：${Date.now() - t}ms`);
    }

    // 更新当前任务的下载状态，同步到配置文件中
    delete task.currentGroupId;
    task.complete = true;
    utils.writeToFile(configPath, JSON.stringify(config, null, 4));
}

module.exports = { startTask };
