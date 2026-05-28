<p align="center">
  <h4 align="center">The perfect starting point to integrate <a href=https://flapjack.com target="_blank">Flapjack</a> within your JavaScript project</h4>

  <p align="center">
    <a href=https://registry.npmjs.org/@flapjack-search/client-search/latest><img src=https://img.shields.io/npm/v/@flapjack-search/client-search.svg?style=flat-square alt="NPM version"></img></a>
    <a href=https://npm-stat.com/charts.html?package=@flapjack-search/client-search><img src=https://img.shields.io/npm/dm/@flapjack-search/client-search.svg?style=flat-square alt="NPM downloads"></a>
    <a href=https://www.jsdelivr.com/package/npm/@flapjack-search/client-search><img src=https://data.jsdelivr.com/v1/package/npm/@flapjack-search/client-search/badge alt="jsDelivr Downloads"></img></a>
    <a href="LICENSE"><img src=https://img.shields.io/badge/license-MIT-green.svg?style=flat-square alt="License"></a>
  </p>
</p>

<p align="center">
  <a href=https://github.com/gridl-staging/flapjack-search-javascript/tree/main/packages/client-search#readme target="_blank">Documentation</a>  •
  <a href=https://github.com/gridl-staging/flapjack-search-javascript/blob/main/MIGRATION.md target="_blank">Migration Guide</a>  •
  <a href=https://github.com/gridl-staging/flapjack-search-javascript/issues target="_blank">Support</a>  •
  <a href=https://github.com/gridl-staging/flapjack-search-javascript/issues/new target="_blank">Open an issue</a>  •
  <a href=https://github.com/gridl-staging/flapjack-search-javascript/issues target="_blank">Report a bug</a>
</p>

## ✨ Features

- Thin & **minimal low-level HTTP client** to interact with Flapjack's API
- Works both on the **browser** and **node.js**
- **UMD and ESM compatible**, you can use it with any module loader
- Built with TypeScript

## 💡 Getting Started

> [!TIP]
> This API client is already a dependency of [the flapjack-search client](https://registry.npmjs.org/flapjack-search/latest), you don't need to manually install `@flapjack-search/client-search` if you already have `flapjack-search` installed.

To get started, you first need to install @flapjack-search/client-search (or any other available API client package).
All of our clients comes with type definition, and are available for both browser and node environments.

### With a package manager


```bash
yarn add @flapjack-search/client-search@beta
# or
npm install @flapjack-search/client-search@beta
# or
pnpm add @flapjack-search/client-search@beta
```

### Without a package manager

Add the following JavaScript snippet to the <head> of your website:

```html
<script src=https://cdn.jsdelivr.net/npm/@flapjack-search/client-search/dist/builds/browser.umd.js></script>
```

### Usage

You can now import the Flapjack API client in your project and play with it.

```js
import { searchClient } from '@flapjack-search/client-search';

const client = searchClient('YOUR_APP_ID', 'YOUR_API_KEY');
```

For full documentation, visit **[client-search README](https://github.com/gridl-staging/flapjack-search-javascript/tree/main/packages/client-search#readme)**.

## ❓ Troubleshooting

Encountering an issue? Open [a GitHub issue](https://github.com/gridl-staging/flapjack-search-javascript/issues/new).

## 📄 License

The Flapjack JavaScript API Client is an open-sourced software licensed under the [MIT license](LICENSE).
