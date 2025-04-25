nodes="10000"
lookahead="0.001"
s="0"
seed="987"
msgs="1000"


#brahms diferent view sizes
#./tests.sh 2 $nodes $msgs -c 1 -update 1 -l $lookahead -seed $seed -sender $s
#./tests.sh 2 $nodes $msgs -c 2 -update 5 -l $lookahead -seed $seed -sender $s
./tests.sh 2 $nodes $msgs -c 2 -update 1 -l $lookahead -seed $seed -sender $s

#./tests.sh 2 $nodes $msgs -c 3 -update 3 -l $lookahead -seed $seed -sender $s
./tests.sh 2 $nodes $msgs -c 3 -update 1 -l $lookahead -seed $seed -sender $s

#./tests.sh 2 $nodes $msgs -c 3 -update 2 -l $lookahead -seed $seed -sender $s

#./tests.sh 2 $nodes $msgs -c 4 -update 5 -l $lookahead -seed $seed -sender $s
./tests.sh 2 $nodes $msgs -c 4 -update 1 -l $lookahead -seed $seed -sender $s
#./tests.sh 2 $nodes $msgs -c 4 -update 2 -l $lookahead -seed $seed -sender $s
./tests.sh 2 $nodes $msgs -c 5 -update 1 -l $lookahead -seed $seed -sender $s
s="1"
#./tests.sh 2 $nodes $msgs -c 5 -update 1 -l $lookahead -seed $seed -sender $s
#./tests.sh 2 $nodes $msgs -c 1 -update 1 -l $lookahead -seed $seed -sender $s
./tests.sh 2 $nodes $msgs -c 2 -update 1 -l $lookahead -seed $seed -sender $s
./tests.sh 2 $nodes $msgs -c 3 -update 1 -l $lookahead -seed $seed -sender $s

./tests.sh 2 $nodes $msgs -c 4 -update 1 -l $lookahead -seed $seed -sender $s
./tests.sh 2 $nodes $msgs -c 5 -update 1 -l $lookahead -seed $seed -sender $s
#HPV diferent view sizes

#./tests.sh 1 $nodes $msgs -c -1 -update 15 -l $lookahead -seed $seed -sender $s
#./tests.sh 1 $nodes $msgs -c -1 -update 10 -l $lookahead -seed $seed -sender $s
#./tests.sh 1 $nodes $msgs -c -2 -update 5 -l $lookahead -seed $seed -sender $s

#./tests.sh 1 $nodes $msgs -c 0 -update 15 -l $lookahead -seed $seed -sender $s
#./tests.sh 1 $nodes $msgs -c 0 -update 10 -l $lookahead -seed $seed -sender $s
#./tests.sh 1 $nodes $msgs -c 0 -update 5 -l $lookahead -seed $seed -sender $s

#./tests.sh 1 $nodes $msgs -c 1 -update 15 -l $lookahead -seed $seed -sender $s
#./tests.sh 1 $nodes $msgs -c 1 -update 10 -l $lookahead -seed $seed -sender $s
#./tests.sh 1 $nodes $msgs -c -1 -update 5 -l $lookahead -seed $seed -sender $s

#./tests.sh 1 $nodes $msgs -c 2 -update 15 -l $lookahead -seed $seed -sender $s 
#./tests.sh 1 $nodes $msgs -c 2 -update 10 -l $lookahead -seed $seed -sender $s 
#./tests.sh 1 $nodes $msgs -c 2 -update 5 -l $lookahead -seed $seed -sender $s
