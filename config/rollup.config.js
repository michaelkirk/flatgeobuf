import resolve from '@rollup/plugin-node-resolve'
import babel from '@rollup/plugin-babel'
import { terser } from 'rollup-plugin-terser'

const plugins = [
  resolve({
    resolveOnly: ['flatbuffers', 'slice-source']
  }),
  babel({
    exclude: 'node_modules/**',
    presets: [['@babel/env', {
      modules: false,
      targets: {
        browsers: ['>2%', 'not dead', 'not ie 11']
      }
    }]],
    babelrc: false,
    babelHelpers: 'bundled'
  }),
  terser()
]

export default [{
  input: './lib/mjs/geojson.js',
  output: {
    file: 'dist/flatgeobuf-geojson.min.js',
    format: 'umd',
    name: 'flatgeobuf',
    sourcemap: true
  },
  plugins
}]
