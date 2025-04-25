nodes="10000"
lookahead="0.001"
s="0"
seed="10"

./tests.sh 3 $nodes 10 -c 3 -update 10 -l $lookahead -sender $s -seed $seed
./tests.sh 3 $nodes 10 -c 3 -update 5 -l $lookahead -sender $s -seed $seed

./tests.sh 1 $nodes 10 -c 1 -update 20 -l $lookahead -sender $s -seed $seed
./tests.sh 1 $nodes 10 -c 1 -update 15 -l $lookahead -sender $s -seed $seed
./tests.sh 2 $nodes 10 -c 3 -update 3 -l $lookahead -sender $s -seed $seed
./tests.sh 2 $nodes 10 -c 3 -update 1 -l $lookahead -sender $s -seed $seed

#brahms diferent view sizes
./tests.sh 2 $nodes 10 -c 2 -update 10 -l $lookahead -sender $s -seed $seed
./tests.sh 2 $nodes 10 -c 2 -update 5 -l $lookahead -sender $s -seed $seed
./tests.sh 2 $nodes 10 -c 2 -update 3 -l $lookahead -sender $s -seed $seed
./tests.sh 2 $nodes 10 -c 2 -update 1 -l $lookahead -sender $s -seed $seed

./tests.sh 2 $nodes 10 -c 4 -update 10 -l $lookahead -sender $s -seed $seed
./tests.sh 2 $nodes 10 -c 4 -update 5 -l $lookahead -sender $s -seed $seed
./tests.sh 2 $nodes 10 -c 4 -update 3 -l $lookahead -sender $s -seed $seed
./tests.sh 2 $nodes 10 -c 4 -update 1 -l $lookahead -sender $s -seed $seed

#HPV diferent view sizes
./tests.sh 1 $nodes 10 -c 0 -update 20 -l $lookahead -sender $s -seed $seed
./tests.sh 1 $nodes 10 -c 0 -update 15 -l $lookahead -sender $s -seed $seed
./tests.sh 1 $nodes 10 -c 0 -update 10 -l $lookahead -sender $s -seed $seed
./tests.sh 1 $nodes 10 -c 0 -update 5 -l $lookahead -sender $s -seed $seed

./tests.sh 1 $nodes 10 -c 2 -update 20 -l $lookahead -sender $s -seed $seed
./tests.sh 1 $nodes 10 -c 2 -update 15 -l $lookahead -sender $s -seed $seed
./tests.sh 1 $nodes 10 -c 2 -update 10 -l $lookahead -sender $s -seed $seed
./tests.sh 1 $nodes 10 -c 2 -update 5 -l $lookahead -sender $s -seed $seed
