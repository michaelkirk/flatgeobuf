import { Feature } from '../feature'
import { Geometry } from '../geometry'
import { GeometryType } from '../geometry-type'
import HeaderMeta from '../HeaderMeta'
import ColumnMeta from '../ColumnMeta'
import { fromGeometry, IGeoJsonGeometry } from './geometry'
import { parseProperties, IFeature } from '../generic/feature'
import Logger from '../Logger'

export interface IGeoJsonProperties {
    [key: string]: boolean | number | string | any
}

export interface IGeoJsonFeature extends IFeature {
    type: string
    geometry: IGeoJsonGeometry
    properties?: IGeoJsonProperties
}

export function fromFeature(feature: Feature, header: HeaderMeta): IGeoJsonFeature {
    const featureColumns = (() => {
        if (feature.columnsLength() == 0) {
            Logger.debug("feature has no per-feature columns");

            const firstColumn = feature.columns(0);
            Logger.debug(`firstColumn: ${firstColumn}`);

            return null;
        }
        const columns: ColumnMeta[] = []
        // CLEANUP: mostly copied from HeaderMeta.ts - can we combine column parsing?
        for (let j = 0; j < feature.columnsLength(); j++) {
            Logger.debug("adding column");
            const column = feature.columns(j)
            if (!column)
                throw new Error('Column unexpectedly missing')
            if (!column.name())
                throw new Error('Column name unexpectedly missing')
            columns.push(new ColumnMeta(column.name() as string, column.type(), column.title(), column.description(), column.width(), column.precision(), column.scale(), column.nullable(), column.unique(), column.primaryKey()))
        }
        return columns;
    })();

    let columns: ColumnMeta[] | null;
    if (featureColumns != null) {
        Logger.debug("using per-feature columns");
        columns = featureColumns;
    } else {
        Logger.debug("using columns from header");
        columns = header.columns;
    }

    Logger.debug(`fromFeature.columns: ${columns}`);
    const geometry = fromGeometry(feature.geometry() as Geometry, header.geometryType)
    Logger.debug(`fromFeature.geometry: ${geometry}`);
    const geoJsonfeature: IGeoJsonFeature = {
        type: 'Feature',
        geometry
    }
    if (columns && columns.length > 0) {
        geoJsonfeature.properties = parseProperties(feature, columns)
    }
    return geoJsonfeature
}
