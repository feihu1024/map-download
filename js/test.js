const utils = require('./utils');
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

    const minzoom = 1;
    const maxzoom = 6;

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

test(12);

// 全球范围：-180,85.0511,179.999999,-85.0511
// 中国范围：72.86133,53.80065,145.81055,1.14250
