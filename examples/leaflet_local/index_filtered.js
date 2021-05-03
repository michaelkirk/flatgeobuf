/* eslint-env browser */
/* global L, flatgeobuf, JSONFormatter */

document.addEventListener("DOMContentLoaded", async () => { 
    // basic OSM Leaflet map
    let map = L.map('map').setView([41.505, -80.09], 4);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 19,
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(map);

    function handleHeaderMeta(headerMeta) {
        const header = document.getElementById('header')
        const formatter = new JSONFormatter(headerMeta, 10)
        header.appendChild(formatter.render())
    }

    const boundingRect = {
        minX: -102,
        maxX: -100,
        minY: 44,
        maxY: 46,
    };

    // use flatgeobuf JavaScript API to iterate features as geojson
    // NOTE: would be more efficient with a special purpose Leaflet deserializer
    let iter = flatgeobuf.deserialize('/test/data/UScounties.fgb', boundingRect, handleHeaderMeta);
    for await (let feature of iter) {
        L.geoJSON(feature).addTo(map);
    }
});
