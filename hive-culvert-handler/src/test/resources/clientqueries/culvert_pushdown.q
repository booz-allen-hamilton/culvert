-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"), you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
-- 
--     http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- 
-- This file is originally taken from the Apache HIVE Project. All modifications
-- are copyright 2011 Booz Allen Hamilton, and licensed under the Apache License,
-- Version 2.0 (the "License"), you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
-- 
--     http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE culvert_pushdown(key int, value string) 
STORED BY 'com.bah.culvert.hive.CulvertStorageHandler'
WITH SERDEPROPERTIES ("culvert.hive.columns.mapping" = ":key,cf:string",
                      "culvert.hive.configuration" = "src/test/resources/culvert-site.xml",
                      "culvert.hive.table" = "culvert_pushdown");
                      

INSERT OVERWRITE TABLE culvert_pushdown 
SELECT *
FROM src;

-- with full pushdown
explain select * from culvert_pushdown where key=90;

select * from culvert_pushdown where key=90;

-- with partial pushdown

explain select * from culvert_pushdown where key=90 and value like '%90%';

select * from culvert_pushdown where key=90 and value like '%90%';

-- with two residuals

explain select * from culvert_pushdown
where key=90 and value like '%90%' and key=cast(value as int);

-- with contradictory pushdowns

explain select * from culvert_pushdown
where key=80 and key=90 and value like '%90%';

select * from culvert_pushdown
where key=80 and key=90 and value like '%90%';

-- with nothing to push down

explain select * from culvert_pushdown;

-- with a predicate which is not actually part of the filter, so
-- it should be ignored by pushdown

explain select * from culvert_pushdown
where (case when key=90 then 2 else 4 end) > 3;

-- with a predicate which is under an OR, so it should
-- be ignored by pushdown

explain select * from culvert_pushdown
where key=80 or value like '%90%';

set hive.optimize.ppd.storage=false;

-- with pushdown disabled

explain select * from culvert_pushdown where key=90;
