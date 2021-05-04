import Logger from "./Logger";

export const NODE_ITEM_LEN: number = 8 * 4 + 8
// default branching factor of a node in the rtree
//
// actual value will be specified in the header but
// this can be useful for having reasonably sized guesses for fetch-sizes when
// streaming results
export const DEFAULT_NODE_SIZE: number = 16;

export interface Rect {
    minX: number
    minY: number
    maxX: number
    maxY: number
}

export function calcTreeSize(numItems: number, nodeSize: number): number {
    nodeSize = Math.min(Math.max(+nodeSize, 2), 65535)
    let n = numItems
    let numNodes = n
    do {
        n = Math.ceil(n / nodeSize)
        numNodes += n
    } while (n !== 1)
    return numNodes * NODE_ITEM_LEN
}

/**
 * returns [leafNodesOffset, numNodes] for each level
 */
function generateLevelBounds(numItems: number, nodeSize: number): Array<[number, number]> {
    if (nodeSize < 2)
        throw new Error('Node size must be at least 2')
    if (numItems === 0)
        throw new Error('Number of items must be greater than 0')

    // number of nodes per level in bottom-up order
    let n = numItems
    let numNodes = n
    const levelNumNodes = [n]
    do {
        n = Math.ceil(n / nodeSize)
        numNodes += n
        levelNumNodes.push(n)
    } while (n !== 1)

    // bounds per level in reversed storage order (top-down)
    const levelOffsets: Array<number> = []
    n = numNodes
    for (const size of levelNumNodes) {
        levelOffsets.push(n - size)
        n -= size
    }
    levelOffsets.reverse()
    levelNumNodes.reverse()
    const levelBounds: Array<[number, number]> = []
    for (let i = 0; i < levelNumNodes.length; i++)
        levelBounds.push([levelOffsets[i], levelOffsets[i] + levelNumNodes[i]])
    levelBounds.reverse()
    return levelBounds
}

type ReadNodeFn = (treeOffset: number, size: number) => Promise<ArrayBuffer>

/** 
 * A feature found to be within the bounding box `rect`
 *
 *  (offset, index)
 *  `offset`: Byte offset in feature data section
 *  `index`: feature number
 */
type SearchResult = [number, number];

export async function* streamSearch(
    numItems: number,
    nodeSize: number,
    rect: Rect,
    readNode: ReadNodeFn): AsyncGenerator<SearchResult, void, unknown>
{
    const { minX, minY, maxX, maxY } = rect
    const levelBounds = generateLevelBounds(numItems, nodeSize)
    const [[leafNodesOffset,numNodes]] = levelBounds
    const queue: any[] = []
    queue.push([0, levelBounds.length - 1])
    while (queue.length !== 0) {
        const [nodeIndex, level] = queue.pop()
        const isLeafNode = nodeIndex >= numNodes - numItems
        // find the end index of the node
        const [,levelBound] = levelBounds[level]
        const end = Math.min(nodeIndex + nodeSize, levelBound)
        const length = end - nodeIndex
        const buffer = await readNode(nodeIndex * NODE_ITEM_LEN, length * NODE_ITEM_LEN)
        const float64Array = new Float64Array(buffer)
        const uint32Array = new Uint32Array(buffer)
        for (let pos = nodeIndex; pos < end; pos++) {
            const nodePos = (pos - nodeIndex) * 5
            if (maxX < float64Array[nodePos + 0]) continue // maxX < nodeMinX
            if (maxY < float64Array[nodePos + 1]) continue // maxY < nodeMinY
            if (minX > float64Array[nodePos + 2]) continue // minX > nodeMaxX
            if (minY > float64Array[nodePos + 3]) continue // minY > nodeMaxY

            const low32Offset = uint32Array[(nodePos << 1) + 8]
            const high32Offset = uint32Array[(nodePos << 1) + 9]
            const offset = readUint52(high32Offset, low32Offset);

            if (isLeafNode)
                yield [offset, pos - leafNodesOffset]
            else
                queue.push([offset, level - 1])
        }
        // order queue to traverse sequential
        queue.sort((a, b) => b[0] - a[0])
    }
}

export async function* httpStreamSearch(
    numItems: number,
    nodeSize: number,
    rect: Rect,
    readNode: ReadNodeFn): AsyncGenerator<SearchResult, void, unknown>
{
    class NodeRange {
        _level: number;
        nodes: [number, number];
        constructor(nodes: [number, number], level: number) {
            this._level = level;
            this.nodes = nodes;
        }

        level(): number {
            return this._level;
        }

        startNode(): number {
            return this.nodes[0];
        }

        endNode(): number {
            return this.nodes[1];
        }

        extendEndNodeToNewOffset(newOffset: number) {
            console.assert(newOffset > this.nodes[1]);
            this.nodes[1] = newOffset;
        }

        toString(): String {
            return `[NodeRange level: ${this._level}, nodes: ${this.nodes[0]}-${this.nodes[1]}]`
        }
    }

    const { minX, minY, maxX, maxY } = rect;
    const levelBounds = generateLevelBounds(numItems, nodeSize);
    const leafNodesOffset = levelBounds[0][0];

    const rootNodeRange: NodeRange = (() => {
        const range: [number, number] = [0, 1];
        const level = levelBounds.length - 1;
        return new NodeRange(range, level);
    })();

    const queue: Array<NodeRange> = [rootNodeRange];

    Logger.debug(`starting stream search with queue: ${queue}, numItems: ${numItems}, nodeSize: ${nodeSize}, levelBounds: ${levelBounds}`);

    while (queue.length != 0) {
        const next = queue.shift()!;

        Logger.debug(`popped node: ${next}, queueLength: ${queue.length}`);

        let nodeIndex = next.startNode()
        const isLeafNode = nodeIndex >= leafNodesOffset

        // find the end index of the node
        const [,levelBound] = levelBounds[next.level()];

        const end = Math.min(next.endNode() + nodeSize, levelBound)
        const length = end - nodeIndex

        const buffer = await readNode(nodeIndex * NODE_ITEM_LEN, length * NODE_ITEM_LEN)

        const float64Array = new Float64Array(buffer)
        const uint32Array = new Uint32Array(buffer)
        for (let pos = nodeIndex; pos < end; pos++) {
            const nodePos = (pos - nodeIndex) * 5
            if (maxX < float64Array[nodePos + 0]) continue // maxX < nodeMinX
            if (maxY < float64Array[nodePos + 1]) continue // maxY < nodeMinY
            if (minX > float64Array[nodePos + 2]) continue // minX > nodeMaxX
            if (minY > float64Array[nodePos + 3]) continue // minY > nodeMaxY

            const low32Offset = uint32Array[(nodePos << 1) + 8]
            const high32Offset = uint32Array[(nodePos << 1) + 9]
            const offset = readUint52(high32Offset, low32Offset);

            if (isLeafNode) {
                Logger.debug("yielding feature");
                yield [offset, pos - leafNodesOffset]
                continue;
            }

            // request up to this many extra bytes if it means we can eliminate
            // an extra request
            const combineRequestThreshold = 256 * 1024 / NODE_ITEM_LEN;

            const tail = queue[queue.length - 1];
            if (tail !== undefined 
                && tail.level() == next.level() - 1
                && offset < tail.endNode() + combineRequestThreshold) {

                Logger.debug(`Extending existing node: ${tail}, newOffset: ${tail.endNode()} -> ${offset}`);
                tail.extendEndNodeToNewOffset(offset);
                continue;
            } else {
                let newNodeRange: NodeRange = (()=> {
                    let level = next.level() - 1;
                    let range: [number, number] = [offset, offset + 1];
                    return new NodeRange(range, level);
                })();

                if (tail !== undefined && tail.level() == next.level() - 1) {
                    Logger.debug(`pushing new node at offset: ${offset} rather than merging with distant ${tail}`);
                } else {
                    Logger.debug(`pushing new level for ${newNodeRange} onto queue with tail: ${tail}`);
                }

                queue.push(newNodeRange);
            }
        }
    }
}

/**
 * Returns a 64-bit uint value by combining it's decomposed lower and higher
 * 32-bit halves. Though because JS `number` is a floating point, it cannot
 * accurately represent an int beyond 52 bits.
 *
 * In practice, "52-bits ought to be enough for anybody", or at least into the
 * pebibytes.
 *
 * Note: `BigInt` does exist to hold larger numbers, but we'd have to adapt a
 * lot of code to support using it.
 */
function readUint52(high32Bits: number, low32Bits: number) {
    // javascript integers can only be 52 bits, verify the top 12 bits
    // are unused.
    if ((high32Bits & 0xfff00000) != 0)  {
        throw Error("integer is too large to be safely represented");
    }

    // Note: we multiply by 2**32 because bitshift operations wrap at 32, so `high32Bits << 32` would be a NOOP.
    const result = low32Bits + (high32Bits * 2**32);

    return result;
}

