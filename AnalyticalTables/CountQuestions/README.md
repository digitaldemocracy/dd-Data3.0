# Measuring Engagement

The purpose of this project was to find an effective way of measuring legislator engagement by counting the number of meaningful interactions they have with witnesses. In order to do this I started by querying the database for interactions that could be potentially meaningful. A full explanation of what this means is provided in the 'GetUtteranceData-Explanation.MD' file. I found it difficult to limit myself to a set of rules for labeling interactions as meaningful, so instead I labeled by hand a small sample of 230 rows. With this data I built several classifiers, an ensemble of which is eventually used to make predictions on the full dataset. These predictions are then aggregated and uploaded to the database.

## Project Overview

### Notebooks
These explain the process of creating and training the classifier. They are not necessary for generating engagement counts, but are helpful for anybody who wants to follow along or make changes.
- **Exploration and Preprocessing.ipynb**
    - This file contains a basic exploration of the dataset as well as some preprocessing for the features. It modifies the dataset and pickles a new version, so it is necessary to be run prior to any of the classifer notebooks.
<br><br>
- **Build Classifier with Context Features.ipynb**
    - File contains the steps for building a classifier based just on the context features, (ie ignoring the step features).
<br><br>
- **Building the Classifier with Text Features.ipynb**
    - Contains the steps for building a classifier that ignores context and looks only at the text features.
<br><br>
- **Weighting Classifiers.ipynb**
    - Takes all of my other classifiers and finds the optimal weights for a voting classifier that I defined.

### Files
The Python files do the actually work of counting enagements and uploading them to the database.
- **GetUtteranceData.py**
    - Performs the initial data processing steps to prepare utterances for classification. Optionally takes two command line arguments:
        - *path to utterance file*
        - *path to classifications file*
    - If no arguments are provided, it will read directly from the db
<br><br>
- **GetUtteranceDataTests.py**
    - Basic unit tests for GetUtteranceData.py
<br><br>
- **GenEngagementScores.py**
    - This script does the work of using the classifier to label all candidate interactions as engagement or not and uses those labels to count engagements for legislators. Note that the classifier is retrained within this script because pickling classifiers and switching architectures can cause problems. This means that the script depends on the labeled data being available to it. Data is then uploaded to the db.
<br><br>
- **EngagementScoresDriver.py**
    - Used to run GetUtteranceData and GenEnagementScores in sequence. Allows for one executable call with a cronjob.
    - *This is the script that should be scheduled*
<br><br>
- **ClassifierFunctions.py**
    - Contains functions and imports used by the classifier notebooks. The final function is used by GenEngagementScores.py for counting engagements.
<br><br>
- **ExploratoryFunctions.py**
    - Contains functions used by the notebooks for preprocessing and data exploration. Final function is imported in both ClassifierFunctions and GenEngagementScores.
- **conn_info.py**
    - Just contains the hard-coded db connection information

### Directories

- **Deprecated**
    - Contains old notebooks. This is just kept for reference.
<br><br>
- **SavedData**
    - This is where all of the data files are written to from the code. Also contains my labeled data set.
<br><br>
- **StanfordNLP**
    - Contains the files necessary for using 'Stanford CoreNLP'. This is not required for getting the  engagement counts, but it is used by some of the notebooks.
<br><br>
- **TestData**
    - Contains the excel files used by the unit tests.