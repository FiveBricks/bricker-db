# Bricker DB
Second iteration of writing database engine from scratch for fun.

## How to run
There is no public API yet. At the moment, I am developing the internals and validating through testing.

## How to run tests
Run all tests with:
```console
$ make test
```

## TODO
- [x] Leaf node data layout
- [x] Internal node data layout
- [x] Paginate nodes
- [ ] B-Tree + Operations
  - [x] Traverse B-tree
  - [x] Insert nodes + propagate changes
  - [ ] Select nodes
  - [ ] Update nodes
  - [ ] Delete nodes
  - [ ] Validate operations
- [ ] Parse SQL
- [ ] Transactions
- [ ] ACID properties?
