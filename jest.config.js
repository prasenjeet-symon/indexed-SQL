module.exports = {
    setupFiles: ['fake-indexeddb/auto'],
    testEnvironment: 'jsdom',
    preset: "jest-puppeteer",
    transform: {
        '^.+\\.ts?$': 'ts-jest'
    },
    testRegex: '/src/tests/.*\\.spec?\\.ts$',
    moduleFileExtensions: ['ts', 'js'],
};