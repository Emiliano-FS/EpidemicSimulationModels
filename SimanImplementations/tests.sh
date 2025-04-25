nodes="-1"
endTime="250"
lookahead="10"
seed="1"
multipleSender="0"
msgs="0"
updateViews="0"
failRate="0"
c="0"
duration="10000"

help()
{
    echo "Usage: CHOICE SIZE MESSAGES
     [ -c VIEWSIZE ] [ -update UPDATEVIEWSTIME ] [ -l LOOKAHEAD ] [ -fr FAILRATE ] [ -seed SEED ] [ -sender [0,1] ]
    CHOICE -> 1 - HyParView, 2 - brahms, 3 - both"
    exit 2
}

VALID_ARGUMENTS=$#
if [ "$VALID_ARGUMENTS" -lt 3 ];then
  if [ "$1" == "clean" ];then
    rm TestsHPV/*
    rm TestsBrahms/*
    exit 2
  else
    help
  fi
fi

nodes=$2
msgs=$3

if  [ "$VALID_ARGUMENTS" -gt 4 ] && [ "$4" == "-c" ]
then
    c=$5
fi


if  [ "$VALID_ARGUMENTS" -gt 6 ] && [ "$6" == "-update" ]
then
    updateViews=$7
fi


if  [ "$VALID_ARGUMENTS" -gt 8 ] && [ "$8" == "-l" ]
then
    lookahead=$9
fi


if  [ "$VALID_ARGUMENTS" -gt 10 ] && [ "${10}" == "-seed" ]
then
    seed=${11}
fi


if  [ "$VALID_ARGUMENTS" -gt 12 ]
then
    if [ "${12}" == "-fr" ]
    then
        failRate=${13}
    elif [ "${12}" == "-sender" ]
    then
        multipleSender=${13}
    fi
fi

if  [ "$VALID_ARGUMENTS" -gt 14 ]
then
    if [ "${14}" == "-fr" ]
    then
        failRate=${15}
    elif [ "${14}" == "-sender" ]
    then
        multipleSender=${15}
    fi
fi




if  [ $1 -eq 1 ]
then
    mkdir TestsHPV
    python3 HyParView.py $nodes $endTime --c $c --updateViews $updateViews -l $lookahead --seedR $seed --msgs $msgs --multipleSender $multipleSender --failRate $failRate &
    psrecord $! --interval 1 --duration $duration --plot TestsHPV/report$nodes-C$c-SD$seed-s$multipleSender-FR$failRate.png
    
elif [ $1 -eq 2 ]
then
    mkdir TestsBrahms
    python3 Brahms.py $nodes $endTime --c $c --updateViews $updateViews -l $lookahead --seedR $seed --msgs $msgs --multipleSender $multipleSender --failRate $failRate &
    psrecord $! --interval 1 --duration $duration --plot TestsBrahms/report$nodes-C$c-SD$seed-s$multipleSender-FR$failRate.png

elif [ $1 -eq 3 ]
then
    mkdir TestsHPV
    mkdir TestsBrahms
    python3 HyParView.py $nodes $endTime --updateViews $updateViews -l $lookahead --seedR $seed --msgs $msgs --multipleSender $multipleSender --failRate $failRate
    python3 Brahms.py $nodes $endTime --c $c --updateViews $updateViews -l $lookahead --seedR $seed --msgs $msgs --multipleSender $multipleSender --failRate $failRate
fi

#python3 HyParView.py $nodes $endTime --updateViews $updateViews -l $lookahead --seedR $seed --msgs $msgs --failRate $failRate


# python3 Brahms.py $nodes $endTime --c 3 --updateViews $updateViews -l $lookahead --seedR $seed --msgs $msgs --failRate $failRate
# python3 Brahms.py $nodes $endTime --c 4 --updateViews $updateViews -l $lookahead --seedR $seed --msgs $msgs --failRate $failRate
#
# python3 Brahms.py $nodes $endTime --c 2 --updateViews 5 -l $lookahead --seedR $seed --msgs $msgs
# python3 Brahms.py $nodes $endTime --c 3 --updateViews 5 -l $lookahead --seedR $seed --msgs $msgs --failRate $failRate
# python3 Brahms.py $nodes $endTime --c 4 --updateViews 5 -l $lookahead --seedR $seed --msgs $msgs --failRate $failRate
