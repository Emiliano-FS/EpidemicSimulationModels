nodes="10000"
lookahead="0.001"
s="1"
seed="11"
msgs="1000"
duration="10000"

#brahms diferent view sizes
#./tests.sh 2 $nodes $msgs -c 2 -update 5 -l $lookahead -seed $seed -sender $s
#./tests.sh 2 $nodes $msgs -c 2 -update 1 -l $lookahead -seed $seed -sender $s
#./tests.sh 2 $nodes $msgs -c 3 -update 3 -l $lookahead -seed $seed -sender $s
#./tests.sh 2 $nodes $msgs -c 3 -update 1 -l $lookahead -seed $seed -sender $s

#HPV diferent view sizes
#./tests.sh 1 $nodes $msgs -c 1 -update 5 -l $lookahead -seed $seed -sender $s

nodes="25000"
#./tests.sh 1 $nodes $msgs -c 1 -update 5 -l $lookahead -seed $seed -sender $s
#./tests.sh 2 $nodes $msgs -c 4 -update 1 -l $lookahead -seed $seed -sender $s
#./tests.sh 2 $nodes $msgs -c 5 -update 1 -l $lookahead -seed $seed -sender $s
nodes="50000"
#./tests.sh 1 $nodes $msgs -c 1 -update 5 -l $lookahead -seed $seed -sender $s
#./tests.sh 2 $nodes $msgs -c 4 -update 1 -l $lookahead -seed $seed -sender $s
./tests.sh 2 $nodes $msgs -c 5 -update 1 -l $lookahead -seed $seed -sender $s
s="0"
#./tests.sh 1 $nodes $msgs -c 1 -update 5 -l $lookahead -seed $seed -sender $s
#./tests.sh 2 $nodes $msgs -c 4 -update 1 -l $lookahead -seed $seed -sender $s
#./tests.sh 2 $nodes $msgs -c 5 -update 1 -l $lookahead -seed $seed -sender $s
nodes="25000"
#./tests.sh 1 $nodes $msgs -c 1 -update 5 -l $lookahead -seed $seed -sender $s
#./tests.sh 2 $nodes $msgs -c 4 -update 1 -l $lookahead -seed $seed -sender $s
#./tests.sh 2 $nodes $msgs -c 5 -update 1 -l $lookahead -seed $seed -sender $s
