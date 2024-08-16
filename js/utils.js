const fs = require('fs');

/** 读取json文件 */
function readJson(filePath) {
    try {
        return JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch (e) {
        console.error(e);
    }
}

/** 写入本地文件 */
function writeToFile(fileName, ...params) {
    const paths = getPaths(fileName);
    paths.forEach((path) => {
        if (!fs.existsSync(path)) {
            fs.mkdirSync(path);
        }
    });
    fs.writeFileSync(fileName, ...params);
}

/** 日志写入本地文件 */
function logToFile(fileName, ...params) {
    const paths = getPaths(fileName);
    paths.forEach((path) => {
        if (!fs.existsSync(path)) {
            fs.mkdirSync(path);
        }
    });
    fs.appendFileSync(fileName, ...params);
}

/** 获取路径 */
function getPaths(fileName) {
    let fileNameString = fileName;
    const pathList = [];

    let splitIndex = fileNameString.indexOf('/');
    while (splitIndex >= 0) {
        const path = fileName.substring(0, splitIndex);
        pathList.push(path);
        fileNameString = fileNameString.replace('/', '-');
        splitIndex = fileNameString.indexOf('/');
    }
    return pathList;
}

/** 经纬度转瓦片行列号 */
function deg2xy(zoom, lon, lat) {
    const x = Math.floor(((lon + 180) / 360) * Math.pow(2, zoom));
    const y = Math.floor(((1 - Math.log(Math.tan((lat * Math.PI) / 180) + 1 / Math.cos((lat * Math.PI) / 180)) / Math.PI) / 2) * Math.pow(2, zoom));
    return [x, y];
}

/** 计算经纬度范围对应的瓦片编号 */
function bounds2Tile(bounds, zoom) {
    const [minX, minY] = deg2xy(zoom, bounds.minX, bounds.minY);
    const [maxX, maxY] = deg2xy(zoom, bounds.maxX, bounds.maxY);
    return { minX, minY, maxX, maxY };
}

function getRandom(m, n = 0) {
    return Math.floor(Math.random() * (m - n)) + n;
}

module.exports = { readJson, writeToFile, logToFile, deg2xy, bounds2Tile, getRandom };
