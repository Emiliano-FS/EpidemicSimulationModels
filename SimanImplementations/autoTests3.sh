nodes="10000"
lookahead="0.001"
msgs="1000"
seed="123"
failRate="0.2"
type="2"
up="1"
s="1"
c="5"

./tests.sh $type $nodes $msgs -c $c -update $up -l $lookahead -seed $seed -fr 0.1 -sender $s
./tests.sh $type $nodes $msgs -c $c -update $up -l $lookahead -seed $seed -fr 0.2 -sender $s
./tests.sh $type $nodes $msgs -c $c -update $up -l $lookahead -seed $seed -fr 0.3 -sender $s
./tests.sh $type $nodes $msgs -c $c -update $up -l $lookahead -seed $seed -fr 0.4 -sender $s
./tests.sh $type $nodes $msgs -c $c -update $up -l $lookahead -seed $seed -fr 0.5 -sender $s
./tests.sh $type $nodes $msgs -c $c -update $up -l $lookahead -seed $seed -fr 0.6 -sender $s
./tests.sh $type $nodes $msgs -c $c -update $up -l $lookahead -seed $seed -fr 0.7 -sender $s
./tests.sh $type $nodes $msgs -c $c -update $up -l $lookahead -seed $seed -fr 0.8 -sender $s
./tests.sh $type $nodes $msgs -c $c -update $up -l $lookahead -seed $seed -fr 0.9 -sender $s

