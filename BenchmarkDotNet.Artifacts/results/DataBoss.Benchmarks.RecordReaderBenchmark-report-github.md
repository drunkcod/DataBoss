```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.2 (25C56) [Darwin 25.2.0]
Apple M2 Pro, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.100
  [Host]     : .NET 10.0.0 (10.0.0, 10.0.25.52411), Arm64 RyuJIT armv8.0-a
  DefaultJob : .NET 10.0.0 (10.0.0, 10.0.25.52411), Arm64 RyuJIT armv8.0-a


```
| Method                           | Mean       | Error   | StdDev  | Ratio | Gen0     | Gen1   | Allocated | Alloc Ratio |
|--------------------------------- |-----------:|--------:|--------:|------:|---------:|-------:|----------:|------------:|
| ObjectDataRecordReader_Benchmark | 1,423.9 μs | 3.92 μs | 3.06 μs |  1.00 | 613.2813 | 5.8594 |    4.9 MB |        1.00 |
| TupleRecordReader_Benchmark      |   569.5 μs | 8.95 μs | 7.94 μs |  0.40 | 218.7500 |      - |   1.78 MB |        0.36 |
