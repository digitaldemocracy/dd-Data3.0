#
# Utterance Log Archival System
#

#Gets today's date
today=$(date +%Y-%m-%d)

echo "Switching user to transcription_tool_common"

# This is kind of unsafe since password is exposed
# Goes into transcription_tool_common's home/logs folder
# Copies log.sql to another file and clears the content of log.sql
echo "mchan18221" | sudo -kS -u transcription_tool_common -H sh -c "cd ;
cd logs ;
cp log.sql log-$today.sql ;
> log.sql
"
echo "Utterance Log Archived for $today"
