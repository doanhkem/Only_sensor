import pickle 
total = 3500
with open('backup.pickle', 'wb') as f:
    pickle.dump(total, f)