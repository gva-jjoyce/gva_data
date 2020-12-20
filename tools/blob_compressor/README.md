LZMA compression can decrease storage costs by about 75% (we're mainly
working with text) but can add orders of magnitude to file save times
(has been observed to take upto 20x longer). File read times are 
less affected with the cost of decompression offset by the cost of
loading the files - with compressed files take approximately 10 to 20%
slower than uncompressed files.

This tool will identify uncompressed data files (.jsonl) in the storage and
replace them with compressed versions (.lzma).