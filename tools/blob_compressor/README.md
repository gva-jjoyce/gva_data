LZMA compression can decrease storage costs by 50% to 90% (usually about 75%
as we're mainly working with jsonl) but can add orders of magnitude to file save
times (has been observed to take upto 20x longer). File read times are 
much less affected with the cost of decompression offset by the cost of
loading the files - with compressed files take approximately 10 to 20%
slower than uncompressed files, but has been observed to be faster.

This tool will identify uncompressed data files (.jsonl) in the storage and
replace them with compressed versions (.lzma).