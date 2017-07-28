# An Overview of dd-Data3.0

**Digital Democracy Database Team**

# Group

**Manager**

* Alex Dekhtyar, Ph.D. (Master of Deep Learning), [dekhtyar@calpoly.edu](mailto:dekhtyar@calpoly.edu)

**Team**

* Nick Russo, [narusso@calpoly.edu](mailto:narusso@calpoly.edu)

* Andrew Rose, [parose@calpoly.edu](mailto:parose@calpoly.edu) 

# Who Do I Ask Questions to:

For emergencies (ex. rm -rf on /var directory), email your manager directly.

For everything else contact Nick Russo, email [nrusso19@gmail.com](mailto:nrusso19@gmail.com) or slack.

If he doesn’t respond, send another email with a parrot emoji![image alt text](image_0.gif). 

If he still doesn’t respond, contact your manager.

# Style Guide

* Follow PEP-8.

* Document code **or else the Pythonista will come after you.**

* Follow the project structure below.

* **Do not write duplicate code. **

* pls do not blindly use list comprehension. pls.

# Project Structure

* **Constants (Python Package)**

    * Allowed items:

        * Constants

        * Queries

    * **No code** should be in this folder.

* **Models (Python Package)**

    * These are all the model representations of data structures

        * Person

        * Legislator

        * Committee etc.

    * **Note:** When adding new data to the project, make a model object. It is important to have these models so it is predictable what objects contain.

* **OpenStatesParsers (Python Package)**

    * This directory contains all parsing classes related to parsing open states data.

    * These classes contain shared code that can be **used for all states.**

        * Override necessary methods for state dependent irregularities.

* **Utils (Python Package)**

    * Contains generic code **used for all states.**

        * Insertion classes

            * Handle all logic for insertion a specific model into the database.

        * Generic Utils

            * Generic repetitive code

            * **Contains logger**

                * **!Note!** Please use this logger, this is a standardized logger.

                    * Printing is not logging. Logging is meant for important messages. For example, a failed insertion or an exception.

                    * Sure, print to debug. I do that too. No one debugs with a logger. 

        * Generic MySQL

            * Standardized Generic MySQL Calls

        * Database Connection

            * Standardized way for connecting to MySQL Database

* **(STATE)-Build**

    * New scripts for importing data

        * This should be extremely minimal. 

        * Most new code should be in the parser.

            * If the new datasource is OpenStates, please review the generic parser that is** already written. **

                * Nothing is worse than duplicate work/code.

        * No new code should be written for inserting data into the database. If you feel there is an instance to add to an insertion manager please contact the person stated above. 

    * Custom implementations of parsers	

        * These are not generic parsers for gathering and formatting data. 

# Adding a New State

#### Steps

1. Gather data sources

    1. OpenStates is a good place to start.

2. Add the new state to State table.

3. Add session year for the new state to Session table.

4. Add the chambers for the new state to the House table (Senate, Assembly, House etc).

5. Minimum data and tables to be filled for Transcription Tool

    2. Legislators

        1. Person

        2. Legislator 

        3. Term

        4. PersonStateAffiliation

        5. AlternateID (If applies)

    3. Committees

        6. Committee

        7. CommitteeNames

        8. ServesOn

        9. author

    4. Bills

        10. Bills

        11. BillVersion

        12. BillVoteSummary

        13. BillVoteDetail

        14. Action

        15. BillAnalysis (If applies)

        16. Motion

    5. Hearings

        17. Hearing

        18. HearingAgenda

        19. CommitteeHearing

    6. Districts

        20. District

    7. Lobbyists

        21. Lobbyists

        22. LobbyingFirm

        23. LobbyingFirmState

        24. LobbyistEmployer

        25. LobbyistEmployment

        26. LobbyistDirectEmployment

        27. LobbyistContract

        28. LobbyistContractWork

        29. PersonStateAffiliation

        30. Organization

6. Consult with Alex Dekhtyar about adding the remaining data into the following tables.

    8. Behest (if applies)

    9. Gifts

    10. Contribution

    11. LegislativeStaff

        31. All tables related.

