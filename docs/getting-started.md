# Getting Started

## Installation

To install `dagger`, simply run this simple command in your terminal of choice:

```bash
python -m pip install py-dagger
```

dagger has a dependency on [faust-streaming](https://github.com/faust-streaming/faust)  for kafka stream processing

## Introduction

The core of `dagger` are Tasks used to define workflows. Dagger uses [faust-streaming](https://github.com/faust-streaming/faust)
to run asynchronous workflows and  uses one faust-supported datastores to store the data.

## FAQ

### Which version of python is supported?

dagger supports python version >= 3.7

### What kafka versions are supported?

dagger supports kafka with version >= 0.10.

## What's Next?

Read the [Usage Guide][usage-guide] for a more detailed descriptions of ways you can use `dagger`.

Read the [API Reference][api-reference] for specific information about all the functions and classes made available by
`dagger`.

[usage-guide]: usage-guide/fundamentals.md
[api-reference]: api.md
