
python BlockpTHyParView.py 500 1000 -l 0.001 --seedR 985
python BlockpTBrahms.py 500 1000 --updateViews 1 --c 5 -l 0.001 --seedR 985
python BlockLazyPush.py 500 1000  -l 0.001  --seedR 985 
python BlockpTDIMPLE.py 500 1000 --shuffleTime 180 -l 0.001 --seedR 985 

python BlockpTHyParView.py 1000 1000 -l 0.001 --seedR 985
python BlockpTBrahms.py 1000 1000 --updateViews 1 --c 5 -l 0.001 --seedR 985
python BlockLazyPush.py 1000 1000  -l 0.001  --seedR 985 
python BlockpTDIMPLE.py 1000 1000 --shuffleTime 180 -l 0.001 --seedR 985 

python BlockpTHyParView.py 2500 1000 -l 0.001 --seedR 985
python BlockpTBrahms.py 2500 1000 --updateViews 1 --c 5 -l 0.001 --seedR 985
python BlockLazyPush.py 2500 1000  -l 0.001  --seedR 985 
python BlockpTDIMPLE.py 2500 1000 --shuffleTime 180 -l 0.001 --seedR 985

python BlockpTHyParView.py 5000 1000 -l 0.001 --seedR 985
python BlockpTBrahms.py 5000 1000 --updateViews 1 --c 5 -l 0.001 --seedR 985
python BlockLazyPush.py 5000 1000  -l 0.001  --seedR 985 
python BlockpTDIMPLE.py 5000 1000 --shuffleTime 180 -l 0.001 --seedR 985 

python BlockpTHyParView.py 10000 1000 -l 0.001 --seedR 985
python BlockpTBrahms.py 10000 1000 --updateViews 1 --c 5 -l 0.001 --seedR 985
python BlockLazyPush.py 10000 1000  -l 0.001  --seedR 985 
python BlockpTDIMPLE.py 10000 1000 --shuffleTime 180 -l 0.001 --seedR 985 

python BlockpTHyParView.py 10000 1000 -l 0.001 --seedR 985 --activeChurn 1 --failRate 0.5
python BlockpTBrahms.py 10000 1000 --updateViews 1 --c 5 -l 0.001 --seedR 985 --activeChurn 1 --failRate 0.5
python BlockLazyPush.py 10000 1000  -l 0.001  --seedR 985 --activeChurn 1 --failRate 0.5
python BlockpTDIMPLE.py 10000 1000 --shuffleTime 180 -l 0.001 --seedR 985 --activeChurn 1 --failRate 0.5

python BlockpTHyParView.py 10000 1000 -l 0.001 --seedR 985  --activeChurn 1 --failRate 0.8
python BlockpTBrahms.py 10000 1000 --updateViews 1 --c 5 -l 0.001 --seedR 985 --activeChurn 1 --failRate 0.8
python BlockLazyPush.py 10000 1000  -l 0.001  --seedR 985 --activeChurn 1 --failRate 0.8
python BlockpTDIMPLE.py 10000 1000 --shuffleTime 180 -l 0.001 --seedR 985 --activeChurn 1 --failRate 0.8

