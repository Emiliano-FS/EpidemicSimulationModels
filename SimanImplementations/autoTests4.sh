nodes="10000"
lookahead="0.1"
s="0"
seed="987"
msgs="1000"


#brahms diferent view sizes
#./tests.sh 1 $nodes $msgs -c 2 -update 5 -l $lookahead -seed $seed -sender $s -fr 0.2
#./tests.sh 1 $nodes $msgs -c 2 -update 5 -l $lookahead -seed $seed -sender $s -fr 0.4
#./tests.sh 1 $nodes $msgs -c 2 -update 5 -l $lookahead -seed $seed -sender $s -fr 0.6
#./tests.sh 1 $nodes $msgs -c 2 -update 5 -l $lookahead -seed $seed -sender $s -fr 0.8
#./tests.sh 1 $nodes $msgs -c 2 -update 5 -l $lookahead -seed $seed -sender $s -fr 0.9

#./tests.sh 2 $nodes $msgs -c 5 -update 1 -l $lookahead -seed $seed -sender $s -fr 0.2
#./tests.sh 2 $nodes $msgs -c 5 -update 1 -l $lookahead -seed $seed -sender $s -fr 0.4
#./tests.sh 2 $nodes $msgs -c 5 -update 1 -l $lookahead -seed $seed -sender $s -fr 0.6
#./tests.sh 2 $nodes $msgs -c 5 -update 1 -l $lookahead -seed $seed -sender $s -fr 0.8
#./tests.sh 2 $nodes $msgs -c 5 -update 1 -l $lookahead -seed $seed -sender $s -fr 0.9

./tests.sh 3 $nodes $msgs  -update 25 -l $lookahead -seed $seed -sender $s -fr 0.2
#./tests.sh 3 $nodes $msgs  -update 25 -l $lookahead -seed $seed -sender $s -fr 0.4
#./tests.sh 3 $nodes $msgs  -update 25 -l $lookahead -seed $seed -sender $s -fr 0.6
#./tests.sh 3 $nodes $msgs  -update 25 -l $lookahead -seed $seed -sender $s -fr 0.8
#./tests.sh 3 $nodes $msgs  -update 25 -l $lookahead -seed $seed -sender $s -fr 0.9
