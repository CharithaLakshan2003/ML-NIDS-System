"""
PASTE THIS AS A NEW CELL in nids_mlp_pipeline.ipynb, immediately AFTER
Step 4 (train/test split) and BEFORE Step 5 (scaling) - i.e. right after
`X_train, X_test, y_train, y_test = train_test_split(...)`.

Why this cell needs to exist at all:
The Flink feature-engineering job cannot safely guess the trained column
order, and it cannot compute a median from a single streaming row (there's
no population to take a median of). Both models were trained on
`feature_names` in a specific order, with NaN/inf already median-imputed
from the FULL training-time dataset - the streaming job needs that exact
order and those exact medians to replicate that imputation on live,
possibly-partial rows, one at a time.

This is not currently saved anywhere in your models/ folder - run this cell
once (or re-run it if you ever retrain on a different dataset/feature set)
before starting the Flink pipeline.

Uses X_train specifically (not the full dataset) so the saved medians
reflect only the training split - consistent with everything else being
fit on X_train alone (scaler, SMOTE, etc.) and not leaking test-set
statistics into what the streaming pipeline treats as "training-time truth".
"""

import json
import os

os.makedirs("models", exist_ok=True)

feature_columns = X_train.columns.tolist()
feature_medians = X_train.median().to_dict()

with open("models/feature_columns.json", "w") as f:
    json.dump(feature_columns, f, indent=2)

with open("models/feature_medians.json", "w") as f:
    json.dump(feature_medians, f, indent=2)

print(f"Saved: models/feature_columns.json  ({len(feature_columns)} columns, in training order)")
print(f"Saved: models/feature_medians.json  ({len(feature_medians)} medians)")
