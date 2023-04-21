use userdb_ecknjpvan
go

drop table if exists #email_domain_name_classification
select a.email_id,
	a.email,
	b.domain_name,
	is_institutional_domain = c.is_institutional_domain_name_after_validation,
	is_student_email = case when c.is_institutional_domain_name_after_validation is null then null when c.is_institutional_domain_name_after_validation = 1 and patindex('%[0-9][0-9][0-9][0-9][0-9]%', a.username) > 0 then 1 else 0 end
into #email_domain_name_classification
from [2023_email_analysis_email] as a
left join [2023_email_analysis_domain] as b on a.full_domain = b.full_domain
left join [2023_email_analysis_domain_classification_validation] as c on b.domain_name = c.domain_name
--14726394

drop table if exists #pub
select a.ut,
	a.pub_year,
	d.LR_main_field_id,
	a.source_id,
	b.publisher_unified_id,
	a.primary_language_id,
	is_gold_oa = case when c.ut is not null then 1 else 0 end,
	a.is_retracted
into #pub
from wos_2239..pub as a
left join wos_2239..pub_publisher as b on a.ut = b.ut
left join wos_2239..pub_open_access_type as c on a.ut = c.ut and c.open_access_type_id = 6  -- Gold OA.
left join
(
	select a.ut, b.LR_main_field_id
	from wos_2213_classification..clustering as a
	join wos_2213_classification..cluster_LR_main_field3 as b on a.cluster_id3 = b.cluster_id3 and b.primary_LR_main_field = 1
) as d on a.ut = d.ut
where a.pub_year between 2004 and 2021
	and a.doc_type_id in (1, 2)  -- Article and review.
--26343025

drop table if exists #pub_field
select ut, LR_main_field_id
into #pub_field
from #pub
--26343025

drop table if exists #pub_author
select a.ut,
	a.author_seq,
	a.email_id,
	is_first_author = case when a.author_seq = 1 then 1 else 0 end,
	is_last_author = case when b.ut is not null then 1 else 0 end,
	is_reprint_author = case when c.ut is not null then 1 else 0 end
into #pub_author
from wos_2239..pub_author as a
left join
(
	select a.ut, max_author_seq = max(a.author_seq)
	from wos_2239..pub_author as a
	join #pub as b on a.ut = b.ut
	group by a.ut
) as b on a.ut = b.ut and a.author_seq = max_author_seq
left join wos_2239..pub_author_reprint as c on a.ut = c.ut and a.author_seq = c.author_seq and c.reprint_seq = 1
join #pub as d on a.ut = d.ut
--136708105

drop table if exists #pub_author_country
select distinct a.ut, a.author_seq, b.country_id
into #pub_author_country
from wos_2239..pub_author_affiliation as a
join wos_2239..pub_affiliation as b on a.ut = b.ut and a.affiliation_seq = b.affiliation_seq
join #pub as c on a.ut = c.ut
where b.country_id is not null
--119583290

drop table if exists #pub_author_country2
select a.ut, a.author_seq, a.country_id, [weight] = cast(1 as float) / b.n_countries
into #pub_author_country2
from #pub_author_country as a
join
(
	select ut, author_seq, n_countries = count(*)
	from #pub_author_country
	group by ut, author_seq
) as b on a.ut = b.ut and a.author_seq = b.author_seq
--119583290

insert into #pub_author_country2 with(tablock)
select a.ut, a.author_seq, null, 1
from #pub_author as a
left join #pub_author_country2 as b on a.ut = b.ut and a.author_seq = b.author_seq
where b.ut is null
--19947400

select count(*), sum([weight])
from #pub_author_country2
--139530690
--136708105

drop table if exists #pub_author_country_email
select a.*, b.author_seq, b.is_first_author, b.is_last_author, b.is_reprint_author, c.country_id, c.[weight], d.email_id, d.domain_name, d.is_institutional_domain, d.is_student_email
into #pub_author_country_email
from #pub as a
join #pub_author as b on a.ut = b.ut
join #pub_author_country2 as c on b.ut = c.ut and b.author_seq = c.author_seq
left join #email_domain_name_classification as d on b.email_id = d.email_id
--139530690

select sum([weight])
from #pub_author_country_email
--136708105


---- Results.

-- Years.
select pub_year,
	avg_n_authorships = cast(n_authorships as float) / n_pubs,
	avg_n_reprint_authorships = cast(n_reprint_authorships as float) / n_pubs,
	avg_n_authorships_with_email = cast(n_authorships_with_email as float) / n_pubs,
	perc_authorships_with_email = cast(n_authorships_with_email as float) / n_authorships,
	perc_first_authorships_with_email = cast(n_first_authorships_with_email as float) / n_first_authorships,
	perc_last_authorships_with_email = cast(n_last_authorships_with_email as float) / n_last_authorships,
	perc_reprint_authorships_with_email = cast(n_reprint_authorships_with_email as float) / n_reprint_authorships,
	perc_reprint_authorships_with_email_institutional_domain = cast(n_reprint_authorships_with_email_institutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_noninstitutional_domain = cast(n_reprint_authorships_with_email_noninstitutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_unkown_domain = cast(n_reprint_authorships_with_email_unknown_domain as float) / n_reprint_authorships_with_email
from
(
	select pub_year,
		n_pubs = count(distinct ut),
		n_authorships = sum([weight]),
		n_first_authorships = sum([weight] * is_first_author),
		n_last_authorships = sum([weight] * is_last_author),
		n_reprint_authorships = sum([weight] * is_reprint_author),
		n_authorships_with_email = sum(case when email_id is not null then [weight] else 0 end),
		n_first_authorships_with_email = sum(case when is_first_author = 1 and email_id is not null then [weight] else 0 end),
		n_last_authorships_with_email = sum(case when is_last_author = 1 and email_id is not null then [weight] else 0 end),
		n_reprint_authorships_with_email = sum(case when is_reprint_author = 1 and email_id is not null then [weight] else 0 end),
		n_reprint_authorships_with_email_institutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 then [weight] else 0 end),
		n_reprint_authorships_with_email_noninstitutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 0 then [weight] else 0 end),
		n_reprint_authorships_with_email_unknown_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain is null then [weight] else 0 end)
	from #pub_author_country_email
	group by pub_year
) as a
order by pub_year

-- Fields (2017-2021).
select b.LR_main_field,
	perc_reprint_authorships_with_email_institutional_domain = cast(n_reprint_authorships_with_email_institutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_noninstitutional_domain = cast(n_reprint_authorships_with_email_noninstitutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_unkown_domain = cast(n_reprint_authorships_with_email_unknown_domain as float) / n_reprint_authorships_with_email
from
(
	select LR_main_field_id,
		n_reprint_authorships_with_email = sum(case when is_reprint_author = 1 and email_id is not null then [weight] else 0 end),
		n_reprint_authorships_with_email_institutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 then [weight] else 0 end),
		n_reprint_authorships_with_email_noninstitutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 0 then [weight] else 0 end),
		n_reprint_authorships_with_email_unknown_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain is null then [weight] else 0 end)
	from #pub_author_country_email
	where pub_year between 2017 and 2021
	group by LR_main_field_id
) as a
left join wos_2213_classification..LR_main_field as b on a.LR_main_field_id = b.LR_main_field_id
order by perc_reprint_authorships_with_email_institutional_domain desc

-- Top 25 countries (2017-2021).
select b.country,
	perc_reprint_authorships_with_email_institutional_domain = cast(n_reprint_authorships_with_email_institutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_noninstitutional_domain = cast(n_reprint_authorships_with_email_noninstitutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_unkown_domain = cast(n_reprint_authorships_with_email_unknown_domain as float) / n_reprint_authorships_with_email
from
(
	select top 25 country_id,
		n_reprint_authorships = sum([weight] * is_reprint_author),
		n_reprint_authorships_with_email = sum(case when is_reprint_author = 1 and email_id is not null then [weight] else 0 end),
		n_reprint_authorships_with_email_institutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 then [weight] else 0 end),
		n_reprint_authorships_with_email_noninstitutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 0 then [weight] else 0 end),
		n_reprint_authorships_with_email_unknown_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain is null then [weight] else 0 end)
	from #pub_author_country_email
	where pub_year between 2017 and 2021
		and country_id is not null
	group by country_id
	order by n_reprint_authorships desc
) as a
left join wos_2239..country as b on a.country_id = b.country_id
order by perc_reprint_authorships_with_email_institutional_domain desc

-- Top 10 languages (2017-2021).
select b.[language],
	perc_reprint_authorships_with_email_institutional_domain = cast(n_reprint_authorships_with_email_institutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_noninstitutional_domain = cast(n_reprint_authorships_with_email_noninstitutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_unkown_domain = cast(n_reprint_authorships_with_email_unknown_domain as float) / n_reprint_authorships_with_email
from
(
	select top 10 primary_language_id,
		n_reprint_authorships = sum([weight] * is_reprint_author),
		n_reprint_authorships_with_email = sum(case when is_reprint_author = 1 and email_id is not null then [weight] else 0 end),
		n_reprint_authorships_with_email_institutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 then [weight] else 0 end),
		n_reprint_authorships_with_email_noninstitutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 0 then [weight] else 0 end),
		n_reprint_authorships_with_email_unknown_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain is null then [weight] else 0 end)
	from #pub_author_country_email
	where pub_year between 2017 and 2021
		and primary_language_id is not null
	group by primary_language_id
	order by n_reprint_authorships desc
) as a
left join wos_2239..[language] as b on a.primary_language_id = b.language_id
order by perc_reprint_authorships_with_email_institutional_domain desc

-- Top 20 publishers (2017-2021).
select b.publisher_unified,
	perc_reprint_authorships_with_email_institutional_domain = cast(n_reprint_authorships_with_email_institutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_noninstitutional_domain = cast(n_reprint_authorships_with_email_noninstitutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_unkown_domain = cast(n_reprint_authorships_with_email_unknown_domain as float) / n_reprint_authorships_with_email
from
(
	select top 20 publisher_unified_id,
		n_reprint_authorships = sum([weight] * is_reprint_author),
		n_reprint_authorships_with_email = sum(case when is_reprint_author = 1 and email_id is not null then [weight] else 0 end),
		n_reprint_authorships_with_email_institutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 then [weight] else 0 end),
		n_reprint_authorships_with_email_noninstitutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 0 then [weight] else 0 end),
		n_reprint_authorships_with_email_unknown_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain is null then [weight] else 0 end)
	from #pub_author_country_email
	where pub_year between 2017 and 2021
		and publisher_unified_id is not null
	group by publisher_unified_id
	order by n_reprint_authorships desc
) as a
left join wos_2239..publisher_unified as b on a.publisher_unified_id = b.publisher_unified_id
order by perc_reprint_authorships_with_email_institutional_domain desc

-- Gold OA (2017-2021).
select oa = case when is_gold_oa = 1 then 'Gold open access' else 'Non-gold open access' end,
	perc_reprint_authorships_with_email_institutional_domain = cast(n_reprint_authorships_with_email_institutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_noninstitutional_domain = cast(n_reprint_authorships_with_email_noninstitutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_unkown_domain = cast(n_reprint_authorships_with_email_unknown_domain as float) / n_reprint_authorships_with_email
from
(
	select is_gold_oa,
		n_reprint_authorships = sum([weight] * is_reprint_author),
		n_reprint_authorships_with_email = sum(case when is_reprint_author = 1 and email_id is not null then [weight] else 0 end),
		n_reprint_authorships_with_email_institutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 then [weight] else 0 end),
		n_reprint_authorships_with_email_noninstitutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 0 then [weight] else 0 end),
		n_reprint_authorships_with_email_unknown_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain is null then [weight] else 0 end)
	from #pub_author_country_email
	where pub_year between 2017 and 2021
	group by is_gold_oa
) as a
order by perc_reprint_authorships_with_email_institutional_domain desc

-- Retracted (2017-2021).
select retracted = case when is_retracted = 1 then 'Retracted' else 'Non-retracted' end,
	perc_reprint_authorships_with_email_institutional_domain = cast(n_reprint_authorships_with_email_institutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_noninstitutional_domain = cast(n_reprint_authorships_with_email_noninstitutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_unkown_domain = cast(n_reprint_authorships_with_email_unknown_domain as float) / n_reprint_authorships_with_email
	--perc_reprint_authorships_with_email_institutional_domain_student = cast(n_reprint_authorships_with_email_institutional_domain_student as float) / n_reprint_authorships_with_email,
	--perc_reprint_authorships_with_email_institutional_domain_nonstudent = cast(n_reprint_authorships_with_email_institutional_domain_nonstudent as float) / n_reprint_authorships_with_email,
	--perc_reprint_authorships_with_email_institutional_domain_student = cast(n_reprint_authorships_with_email_institutional_domain_student as float) / n_reprint_authorships_with_email_institutional_domain,
	--perc_reprint_authorships_with_email_institutional_domain_nonstudent = cast(n_reprint_authorships_with_email_institutional_domain_nonstudent as float) / n_reprint_authorships_with_email_institutional_domain
from
(
	select is_retracted,
		n_reprint_authorships = sum([weight] * is_reprint_author),
		n_reprint_authorships_with_email = sum(case when is_reprint_author = 1 and email_id is not null then [weight] else 0 end),
		n_reprint_authorships_with_email_institutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 then [weight] else 0 end),
		n_reprint_authorships_with_email_noninstitutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 0 then [weight] else 0 end),
		n_reprint_authorships_with_email_unknown_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain is null then [weight] else 0 end)
		--n_reprint_authorships_with_email_institutional_domain_student = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 and is_student_email = 1 then [weight] else 0 end),
		--n_reprint_authorships_with_email_institutional_domain_nonstudent = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 and is_student_email = 0 then [weight] else 0 end)
	from #pub_author_country_email
	where pub_year between 2017 and 2021
	group by is_retracted
) as a
order by perc_reprint_authorships_with_email_institutional_domain desc

-- Top 25 non-institutional email domains (2017-2021).
select domain_name, n_reprint_authorships
from
(
	select top 25 domain_name,
		n_pubs = count(distinct ut),
		n_reprint_authorships = sum([weight])
	from #pub_author_country_email
	where pub_year between 2017 and 2021
		and is_reprint_author = 1
		and is_institutional_domain = 0
		--and country_id = 44  -- China
		--and country_id = 222 -- USA
		--and country_id = 78  -- Germany
		--and country_id = 171 -- Russia
		--and country_id = 95  -- India
		--and country_id = 146 -- Netherlands
		--and country_id = 21  -- Belgium
	group by domain_name
	order by n_reprint_authorships desc
) as a
order by n_reprint_authorships desc

-- Overall publication and authorship statistics (2017-2021).
select
	n_pubs,
	n_authorships,
	n_authorships_with_email,
	perc_authorships_with_email = cast(n_authorships_with_email as float) / n_authorships,
	n_reprint_authorships,
	n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email = cast(n_reprint_authorships_with_email as float) / n_reprint_authorships,
	n_reprint_authorships_with_email_institutional_domain,
	n_reprint_authorships_with_email_noninstitutional_domain,
	n_reprint_authorships_with_email_unknown_domain,
	perc_reprint_authorships_with_email_institutional_domain = cast(n_reprint_authorships_with_email_institutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_noninstitutional_domain = cast(n_reprint_authorships_with_email_noninstitutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_unkown_domain = cast(n_reprint_authorships_with_email_unknown_domain as float) / n_reprint_authorships_with_email
from
(
	select n_pubs = count(distinct ut),
		n_authorships = sum([weight]),
		n_authorships_with_email = sum(case when email_id is not null then [weight] else 0 end),
		n_first_authorships = sum([weight] * is_first_author),
		n_first_authorships_with_email = sum(case when is_first_author = 1 and email_id is not null then [weight] else 0 end),
		n_last_authorships = sum([weight] * is_last_author),
		n_last_authorships_with_email = sum(case when is_last_author = 1 and email_id is not null then [weight] else 0 end),
		n_reprint_authorships = sum([weight] * is_reprint_author),
		n_reprint_authorships_with_email = sum(case when is_reprint_author = 1 and email_id is not null then [weight] else 0 end),
		n_reprint_authorships_with_email_institutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 then [weight] else 0 end),
		n_reprint_authorships_with_email_noninstitutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 0 then [weight] else 0 end),
		n_reprint_authorships_with_email_unknown_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain is null then [weight] else 0 end)
	from #pub_author_country_email
	where pub_year between 2017 and 2021
) as a

-- Validation statistics.
select count(*),
	sum(case when is_institutional_domain_name_after_validation = 1 then 1 else 0 end),
	sum(case when is_institutional_domain_name_after_validation = 0 then 1 else 0 end),
	sum(case when is_institutional_domain_name_after_validation is null then 1 else 0 end),
	cast(sum(case when is_institutional_domain_name_after_validation is null then 1 else 0 end) as float) / count(*),
	cast(sum(case when is_institutional_domain_name_after_validation is null then n_emails else 0 end) as float) / sum(n_emails)
from [2023_email_analysis_domain_classification_validation]
--11608
--10838
--606
--164
--0.0141281
--0.00249826512951634

-- Assigned email address statistics.
select count(*),
	sum(case when is_institutional_domain is not null then 1 else 0 end),
	cast(sum(case when is_institutional_domain is not null then 1 else 0 end) as float) / count(*)
from #email_domain_name_classification
--14726394
--13022581
--0.884302090518561

-- Random sample of non-assigned email addresses.
select top 1000
	b.email,
	b.has_valid_format,
	b.has_matched_suffix,
	is_below_threshold = case when d.domain_name is null then 1 else 0 end,
	b.username,
	c.subdomain,
	c.domain_name
from #pub_author_country_email as a
left join [2023_email_analysis_email] as b on a.email_id = b.email_id
left join [2023_email_analysis_domain] as c on b.full_domain = c.full_domain
left join [2023_email_analysis_domain_classification_validation] as d on c.domain_name = d.domain_name
where is_reprint_author = 1
	and a.email_id is not null
	and a.is_institutional_domain is null
	and pub_year between 2017 and 2021
order by newid()
