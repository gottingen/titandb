# titan-redis

titan-redis is a disk redis engine based on rocksdb and I make it wisc -key an provide redis interface.

**build**
```shell
conda env create -f conda/enviroment_linux.yaml
conda acttivate titandb-dev
mkdir build
cd build
cmake ..
make
```

# install

```shell
conda install -c titan-search titandb
```