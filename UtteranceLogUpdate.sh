#
# Utterance Log Archival System
#

#Gets today's date
today=$(date +%Y-%m-%d)

# Goes into transcription_tool_common's home/logs folder
# Copies log.sql to another file and clears the content of log.sql
cd logs
cp sql.log UtterLog-$today.sql
> sql.log

echo "Utterance Log Archived for $today"
