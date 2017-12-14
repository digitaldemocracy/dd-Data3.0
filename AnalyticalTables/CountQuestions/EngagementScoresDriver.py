"""Used to run GetUtteranceData and GenEnagementScores in sequence. Allows for one executable call with a
   cron job."""

import GetUtteranceData
import GenEngagementScores


def main():
    GetUtteranceData.main()
    GenEngagementScores.main()


if __name__ == '__main__':
    main()