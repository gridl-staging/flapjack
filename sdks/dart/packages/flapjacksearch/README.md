<p align="center">
  <h3 align="center">Flapjack Dart API Client</h3>
</p>

<p align="center">
  <a href=https://github.com/gridl-hq/flapjack/tree/main/sdks/dart/packages/flapjacksearch><img src=https://img.shields.io/pub/v/flapjacksearch.svg alt="Latest version"/></a>
  <a href=https://github.com/gridl-hq/flapjack/tree/main/sdks/dart/packages/flapjacksearch><img src=https://img.shields.io/pub/publisher/flapjacksearch.svg alt="Publisher"/></a>
</p>

## Description

`flapjacksearch` is the main Dart package for working with Flapjack search and insights APIs in Dart and Flutter projects.

The package currently provides:

- Search client for query and index operations.
- Insights client for click and conversion event tracking.
- Search-lite import for search-only projects.

## Features

- Thin low-level HTTP clients for Flapjack APIs.
- Native and web support.
- Pure Dart implementation.

## Getting Started

### Step 1: Add Dependency

For Dart projects:

```shell
dart pub add flapjacksearch
```

For Flutter projects:

```shell
flutter pub add flapjacksearch
```

### Step 2: Import the Package

```dart
import 'package:flapjacksearch/flapjacksearch.dart';
```

For search-only usage:

```dart
import 'package:flapjacksearch/flapjacksearch_lite.dart';
```

## License

Flapjack Dart API Client is open source software licensed under the [MIT license](LICENSE).
