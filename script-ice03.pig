
REGISTER ./tutorial.jar; 

raw = LOAD 'excite.log' USING PigStorage('\t') AS (user, time, query);

clean1 = FILTER raw BY org.apache.pig.tutorial.NonURLDetector(query);

clean2 = FOREACH clean1 GENERATE user, time, org.apache.pig.tutorial.ToLower(query) as query;

houred = FOREACH clean2 GENERATE user, org.apache.pig.tutorial.ExtractHour(time) as hour, query;

ngramed1 = FOREACH houred GENERATE user, hour, flatten(org.apache.pig.tutorial.NGramGenerator(query)) as ngram;

ngramed2 = DISTINCT ngramed1;

hour_frequency1 = GROUP ngramed2 BY (ngram, hour);

hour_frequency2 = FOREACH hour_frequency1 GENERATE flatten($0), COUNT($1) as count;

uniq_frequency1 = GROUP hour_frequency2 BY group::ngram;

uniq_frequency2 = FOREACH uniq_frequency1 GENERATE flatten($0), flatten(org.apache.pig.tutorial.ScoreGenerator($1));

uniq_frequency3 = FOREACH uniq_frequency2 GENERATE $1 as hour, $0 as ngram, $2 as score, $3 as count, $4 as mean;

filtered_uniq_frequency = FILTER uniq_frequency3 BY score > 2.0;

ordered_uniq_frequency = ORDER filtered_uniq_frequency BY (hour, score);

STORE ordered_uniq_frequency INTO '/tmp/query-results' USING PigStorage(); 



-- Answering 2nd question
five_pm_seven_am_Freq = FILTER ordered_uniq_frequency BY (hour >= 17) OR (hour <= 7);

STORE five_pm_seven_am_Freq INTO '/tmp/question2-results' USING PigStorage(); 


-- Answering 3rd question
filtered_uniq_frequency3 = FILTER uniq_frequency3 BY score > 1.0;

ordered_uniq_frequency3 = ORDER filtered_uniq_frequency3 BY (hour, score);

STORE ordered_uniq_frequency3 INTO '/tmp/question3-results' USING PigStorage();


-- Answering 4th question
ordered_uniq_frequency4 = ORDER uniq_frequency3 BY (hour, score);

STORE ordered_uniq_frequency4 INTO '/tmp/question4-results' USING PigStorage();