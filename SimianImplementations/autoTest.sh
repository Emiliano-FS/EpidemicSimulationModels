
# Bitcoin Core (Baseline)
python BlockLazyPush.py 5000  1000 --seedR 10 -l 0.001 --activeChurn 0
python BlockLazyPush.py 10000 1000 --seedR 10 -l 0.001 --activeChurn 0
python BlockLazyPush.py 15000 1000 --seedR 10 -l 0.001 --activeChurn 0
python BlockLazyPush.py 20000 1000 --seedR 10 -l 0.001 --activeChurn 0

# PlumTree + HyParView
python BlockpTHyParView.py 5000  1000 --seedR 10 -l 0.001 --activeChurn 0 --c 2 --arwl 6 --prwl 3 --updateViews 5
python BlockpTHyParView.py 10000 1000 --seedR 10 -l 0.001 --activeChurn 0 --c 2 --arwl 6 --prwl 3 --updateViews 5
python BlockpTHyParView.py 15000 1000 --seedR 10 -l 0.001 --activeChurn 0 --c 2 --arwl 6 --prwl 3 --updateViews 5
python BlockpTHyParView.py 20000 1000 --seedR 10 -l 0.001 --activeChurn 0 --c 2 --arwl 6 --prwl 3 --updateViews 5

# PlumTree + Brahms
python BlockpTBrahms.py 5000  1000 --seedR 10 -l 0.001 --activeChurn 0 --updateViews 5 --c 1
python BlockpTBrahms.py 10000 1000 --seedR 10 -l 0.001 --activeChurn 0 --updateViews 5 --c 1
python BlockpTBrahms.py 15000 1000 --seedR 10 -l 0.001 --activeChurn 0 --updateViews 5 --c 1
python BlockpTBrahms.py 20000 1000 --seedR 10 -l 0.001 --activeChurn 0 --updateViews 5 --c 1

# PlumTree + DIMPLE
python BlockpTDIMPLE.py 5000  1000 --seedR 10 -l 0.001 --activeChurn 0 --shuffleTime 25
python BlockpTDIMPLE.py 10000 1000 --seedR 10 -l 0.001 --activeChurn 0 --shuffleTime 25
python BlockpTDIMPLE.py 15000 100 --seedR 10 -l 0.001 --activeChurn 0 --shuffleTime 25
python BlockpTDIMPLE.py 20000 100 --seedR 10 -l 0.001 --activeChurn 0 --shuffleTime 25
