import del from 'del';
import {
    terser
} from 'rollup-plugin-terser';
import typescript from 'rollup-plugin-typescript2';

export default async function ({
    watch
}) {
    await del('build');

    const builds = [];

    builds.push({
        plugins: [
            typescript({
                typescript: require('typescript'),
            }),
        ],
        input: ['src/index.ts'],
        output: [{
                dir: 'build/esm/',
                format: 'esm',
                entryFileNames: '[name].js',
                chunkFileNames: '[name].js',
            },
            {
                dir: 'build/cjs/',
                format: 'cjs',
                entryFileNames: '[name].js',
                chunkFileNames: '[name].js',
            },
        ],
    });

    // Minified iife
    builds.push({
        input: 'build/esm/index.js',
        plugins: [
            terser({
                compress: {
                    ecma: 2019
                },
            }),
        ],
        output: {
            file: 'build/iife/index-min.js',
            format: 'iife',
            name: 'ipdb',
        },
    });

    return builds
}