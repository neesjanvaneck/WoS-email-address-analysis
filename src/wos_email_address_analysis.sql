use userdb_ecknjpvan
go

drop table if exists #email_domain_name_classification
select a.email_id,
	a.email,
	b.domain_name,
	is_institutional_domain = c.is_institutional_domain_name_after_validation,
	is_student_email = case when c.is_institutional_domain_name_after_validation is null then null when c.is_institutional_domain_name_after_validation = 1 and patindex('%[0-9][0-9][0-9][0-9][0-9]%', a.username) > 0 then 1 else 0 end
into #email_domain_name_classification
from [2025_email_analysis_email] as a
left join [2025_email_analysis_domain] as b on a.full_domain = b.full_domain
left join [2025_email_analysis_domain_classification_validation] as c on b.domain_name = c.domain_name
--14726394
--19951961

drop table if exists #pub
select a.ut,
	a.doi,
	a.pub_year,
	d.main_field_id,
	a.source_id,
	b.publisher_unified_id,
	a.primary_language_id,
	is_gold_oa = case when c.ut is not null then 1 else 0 end,
	is_retracted = case when e.doi is not null then 1 else 0 end
into #pub
from wos_2513..pub as a
left join wos_2513..pub_publisher as b on a.ut = b.ut
left join wos_2513..pub_open_access_type as c on a.ut = c.ut and c.open_access_type_id = 6  -- Gold OA.
left join
(
	select a.ut, b.main_field_id
	from wos_2513_classification..clustering as a
	join wos_2513_classification..micro_cluster_main_field as b on a.micro_cluster_id = b.micro_cluster_id and b.is_primary_main_field = 1
) as d on a.ut = d.ut
left join
(
	select distinct doi = originalpaperdoi
	from retractions_20250602
	where originalpaperdoi is not null
) as e on a.doi = e.doi
where a.pub_year between 2005 and 2024
	and a.doc_type_id in (1, 2)  -- Article and review.
--26343025
--38851927

drop table if exists #pub_field
select ut, main_field_id
into #pub_field
from #pub
--26343025
--38851927

drop table if exists #pub_author
select a.ut,
	a.author_seq,
	a.email_id,
	is_first_author = case when a.author_seq = 1 then 1 else 0 end,
	is_last_author = case when b.ut is not null then 1 else 0 end,
	is_reprint_author = case when c.ut is not null then 1 else 0 end
into #pub_author
from wos_2513..pub_author as a
left join
(
	select a.ut, max_author_seq = max(a.author_seq)
	from wos_2513..pub_author as a
	join #pub as b on a.ut = b.ut
	group by a.ut
) as b on a.ut = b.ut and a.author_seq = max_author_seq
left join
(
	select distinct ut, author_seq
	from wos_2513..pub_author_reprint
) as c on a.ut = c.ut and a.author_seq = c.author_seq
join #pub as d on a.ut = d.ut
--136708105
--195152152

drop table if exists #pub_author_country
select distinct a.ut, a.author_seq, b.country_id
into #pub_author_country
from wos_2513..pub_author_affiliation as a
join wos_2513..pub_affiliation as b on a.ut = b.ut and a.affiliation_seq = b.affiliation_seq
join #pub as c on a.ut = c.ut
where b.country_id is not null
union
select distinct a.ut, a.author_seq, b.country_id
from wos_2513..pub_author_reprint as a
join wos_2513..pub_reprint as b on a.ut = b.ut and a.reprint_seq = b.reprint_seq
join #pub as c on a.ut = c.ut
where b.country_id is not null
--119583290
--185668650

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
--185668650

insert into #pub_author_country2 with(tablock)
select a.ut, a.author_seq, null, 1
from #pub_author as a
left join #pub_author_country2 as b on a.ut = b.ut and a.author_seq = b.author_seq
where b.ut is null
--19947400
--13859122

select count(*), sum([weight])
from #pub_author_country2
--139530690, 136708105
--199527772, 195152152.000085

drop table if exists #pub_author_country_email
select a.*, b.author_seq, b.is_first_author, b.is_last_author, b.is_reprint_author, c.country_id, c.[weight], d.email_id, d.domain_name, d.is_institutional_domain, d.is_student_email
into #pub_author_country_email
from #pub as a
join #pub_author as b on a.ut = b.ut
join #pub_author_country2 as c on b.ut = c.ut and b.author_seq = c.author_seq
left join #email_domain_name_classification as d on b.email_id = d.email_id
--139530690
--199527772

select sum([weight])
from #pub_author_country_email
--136708105
--195152152.000055



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

-- Fields (2020-2024).
select b.main_field,
	perc_reprint_authorships_with_email_institutional_domain = cast(n_reprint_authorships_with_email_institutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_noninstitutional_domain = cast(n_reprint_authorships_with_email_noninstitutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_unkown_domain = cast(n_reprint_authorships_with_email_unknown_domain as float) / n_reprint_authorships_with_email
from
(
	select main_field_id,
		n_reprint_authorships_with_email = sum(case when is_reprint_author = 1 and email_id is not null then [weight] else 0 end),
		n_reprint_authorships_with_email_institutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 then [weight] else 0 end),
		n_reprint_authorships_with_email_noninstitutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 0 then [weight] else 0 end),
		n_reprint_authorships_with_email_unknown_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain is null then [weight] else 0 end)
	from #pub_author_country_email
	where pub_year between 2020 and 2024
	group by main_field_id
) as a
left join wos_2513_classification..main_field as b on a.main_field_id = b.main_field_id
order by perc_reprint_authorships_with_email_institutional_domain desc

-- Top 25 countries (2020-2024).
drop table if exists #country_results
select country_id,
	perc_reprint_authorships_with_email_institutional_domain = cast(n_reprint_authorships_with_email_institutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_noninstitutional_domain = cast(n_reprint_authorships_with_email_noninstitutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_unkown_domain = cast(n_reprint_authorships_with_email_unknown_domain as float) / n_reprint_authorships_with_email
into #country_results
from
(
	select top 25 country_id,
		n_reprint_authorships = sum([weight] * is_reprint_author),
		n_reprint_authorships_with_email = sum(case when is_reprint_author = 1 and email_id is not null then [weight] else 0 end),
		n_reprint_authorships_with_email_institutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 then [weight] else 0 end),
		n_reprint_authorships_with_email_noninstitutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 0 then [weight] else 0 end),
		n_reprint_authorships_with_email_unknown_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain is null then [weight] else 0 end)
	from #pub_author_country_email
	where pub_year between 2020 and 2024
		and country_id is not null
	group by country_id
	order by n_reprint_authorships desc
) as a

select b.country,
	perc_reprint_authorships_with_email_institutional_domain,
	perc_reprint_authorships_with_email_noninstitutional_domain,
	perc_reprint_authorships_with_email_unkown_domain
from #country_results as a
join wos_2513..country as b on a.country_id = b.country_id
order by perc_reprint_authorships_with_email_institutional_domain desc

-- Top 25 countries vs fields (2020-2024).
select *
from
(
	select b.country,
		main_field = 'All',
		perc_reprint_authorships_with_email_institutional_domain
	from #country_results as a
	join wos_2513..country as b on a.country_id = b.country_id
	union all
	select b.country,
		c.main_field,
		perc_reprint_authorships_with_email_institutional_domain = cast(n_reprint_authorships_with_email_institutional_domain as float) / n_reprint_authorships_with_email
	from
	(
		select a.country_id,
			a.main_field_id,
			n_reprint_authorships = sum([weight] * is_reprint_author),
			n_reprint_authorships_with_email = sum(case when is_reprint_author = 1 and email_id is not null then [weight] else 0 end),
			n_reprint_authorships_with_email_institutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 then [weight] else 0 end),
			n_reprint_authorships_with_email_noninstitutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 0 then [weight] else 0 end),
			n_reprint_authorships_with_email_unknown_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain is null then [weight] else 0 end)
		from #pub_author_country_email as a
		join #country_results as b on a.country_id = b.country_id
		where a.pub_year between 2020 and 2024
		group by a.country_id, a.main_field_id
	) as a
	left join wos_2513..country as b on a.country_id = b.country_id
	left join wos_2513_classification..main_field as c on a.main_field_id = c.main_field_id
	--order by country, main_field
) as a
pivot
(
	sum(perc_reprint_authorships_with_email_institutional_domain)
	for main_field in ([All], [Biomedical and health sciences], [Life and earth sciences], [Mathematics and computer science], [Physical sciences and engineering], [Social sciences and humanities])
) as b
order by [All]

-- Top 10 languages (2020-2024).
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
	where pub_year between 2020 and 2024
		and primary_language_id is not null
	group by primary_language_id
	order by n_reprint_authorships desc
) as a
left join wos_2513..[language] as b on a.primary_language_id = b.language_id
order by perc_reprint_authorships_with_email_institutional_domain desc

-- Top 20 publishers (2020-2024).
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
	where pub_year between 2020 and 2024
		and publisher_unified_id is not null
	group by publisher_unified_id
	order by n_reprint_authorships desc
) as a
left join wos_2513..publisher_unified as b on a.publisher_unified_id = b.publisher_unified_id
order by perc_reprint_authorships_with_email_institutional_domain desc

-- Gold OA (2020-2024).
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
	where pub_year between 2020 and 2024
	group by is_gold_oa
) as a
order by perc_reprint_authorships_with_email_institutional_domain desc

-- Retracted (2020-2024).
drop table if exists #retracted_results
select retractied_status = case when is_retracted = 1 then 'Retracted' else 'Non-retracted' end,
	perc_reprint_authorships_with_email_institutional_domain = cast(n_reprint_authorships_with_email_institutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_noninstitutional_domain = cast(n_reprint_authorships_with_email_noninstitutional_domain as float) / n_reprint_authorships_with_email,
	perc_reprint_authorships_with_email_unkown_domain = cast(n_reprint_authorships_with_email_unknown_domain as float) / n_reprint_authorships_with_email
	--perc_reprint_authorships_with_email_institutional_domain_student = cast(n_reprint_authorships_with_email_institutional_domain_student as float) / n_reprint_authorships_with_email,
	--perc_reprint_authorships_with_email_institutional_domain_nonstudent = cast(n_reprint_authorships_with_email_institutional_domain_nonstudent as float) / n_reprint_authorships_with_email,
	--perc_reprint_authorships_with_email_institutional_domain_student = cast(n_reprint_authorships_with_email_institutional_domain_student as float) / n_reprint_authorships_with_email_institutional_domain,
	--perc_reprint_authorships_with_email_institutional_domain_nonstudent = cast(n_reprint_authorships_with_email_institutional_domain_nonstudent as float) / n_reprint_authorships_with_email_institutional_domain
into #retracted_results
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
	where pub_year between 2020 and 2024
	group by is_retracted
) as a
order by perc_reprint_authorships_with_email_institutional_domain desc

select retractied_status,
	perc_reprint_authorships_with_email_institutional_domain,
	perc_reprint_authorships_with_email_noninstitutional_domain,
	perc_reprint_authorships_with_email_unkown_domain
from #retracted_results
order by perc_reprint_authorships_with_email_institutional_domain desc

-- Retracted vs fields (2020-2024).
select *
from
(
	select main_field = 'All',
		retractied_status,
		perc_reprint_authorships_with_email_noninstitutional_domain
	from #retracted_results
	union all
	select b.main_field,
		retractied_status = case when a.is_retracted = 1 then 'Retracted' else 'Non-retracted' end,
		perc_reprint_authorships_with_email_noninstitutional_domain = cast(n_reprint_authorships_with_email_noninstitutional_domain as float) / n_reprint_authorships_with_email
	from
	(
		select main_field_id,
			is_retracted,
			n_reprint_authorships_with_email = sum(case when is_reprint_author = 1 and email_id is not null then [weight] else 0 end),
			n_reprint_authorships_with_email_institutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 then [weight] else 0 end),
			n_reprint_authorships_with_email_noninstitutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 0 then [weight] else 0 end),
			n_reprint_authorships_with_email_unknown_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain is null then [weight] else 0 end)
		from #pub_author_country_email
		where pub_year between 2020 and 2024
		group by main_field_id, is_retracted
	) as a
	left join wos_2513_classification..main_field as b on a.main_field_id = b.main_field_id
) as a
pivot
(
	sum(perc_reprint_authorships_with_email_noninstitutional_domain)
	for retractied_status in ([Retracted], [Non-retracted])
) as b
order by [Retracted] desc

-- Retracted vs top 25 countries (2020-2024).
select *
from
(
	select country = 'All',
		retractied_status,
		perc_reprint_authorships_with_email_noninstitutional_domain
	from #retracted_results
	union all
	select b.country,
		retractied_status = case when a.is_retracted = 1 then 'Retracted' else 'Non-retracted' end,
		perc_reprint_authorships_with_email_noninstitutional_domain = cast(n_reprint_authorships_with_email_noninstitutional_domain as float) / n_reprint_authorships_with_email
	from
	(
		select a.country_id,
			a.is_retracted,
			n_reprint_authorships = sum([weight] * is_reprint_author),
			n_reprint_authorships_with_email = sum(case when is_reprint_author = 1 and email_id is not null then [weight] else 0 end),
			n_reprint_authorships_with_email_institutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 1 then [weight] else 0 end),
			n_reprint_authorships_with_email_noninstitutional_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain = 0 then [weight] else 0 end),
			n_reprint_authorships_with_email_unknown_domain = sum(case when is_reprint_author = 1 and email_id is not null and is_institutional_domain is null then [weight] else 0 end)
		from #pub_author_country_email as a
		join #country_results as b on a.country_id = b.country_id
		where a.pub_year between 2020 and 2024
		group by a.country_id, a.is_retracted
	) as a
	left join wos_2513..country as b on a.country_id = b.country_id
) as a
pivot
(
	sum(perc_reprint_authorships_with_email_noninstitutional_domain)
	for retractied_status in ([Retracted], [Non-retracted])
) as b
order by [Retracted] desc

-- Top 25 non-institutional email domains (2020-2024).
select a.domain_name, company = replace(b.company, '"', '') + ' (' + b.company_lowest_level_country + ')', a.n_reprint_authorships
from
(
	select top 25 domain_name,
		n_pubs = count(distinct ut),
		n_reprint_authorships = sum([weight])
	from #pub_author_country_email
	where pub_year between 2020 and 2024
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
left join [2025_email_analysis_domain_classification_validation] as b on a.domain_name = b.domain_name
order by a.n_reprint_authorships desc

-- Top 25 reprint author countries vs top 12 non-institutional email provider countries.
drop table if exists #country_reprint_author
select c.country, n_reprint_authorships = sum([weight] * is_reprint_author)
into #country_reprint_author
from #pub_author_country_email as a
join #country_results as b on a.country_id = b.country_id
join wos_2513..country as c on a.country_id = c.country_id
where pub_year between 2020 and 2024
	and is_reprint_author = 1
	and is_institutional_domain = 0
group by c.country
order by n_reprint_authorships desc
--25

drop table if exists #country_email_provider
select d.company_lowest_level_country, n_reprint_authorships = sum([weight] * is_reprint_author)
into #country_email_provider
from #pub_author_country_email as a
join #country_results as b on a.country_id = b.country_id
join [2025_email_analysis_domain_classification_validation] as d on a.domain_name = d.domain_name
where pub_year between 2020 and 2024
	and is_reprint_author = 1
	and is_institutional_domain = 0
group by d.company_lowest_level_country
order by n_reprint_authorships desc
--50

drop table if exists #country_reprint_author_email_provider
select c.country, d.company_lowest_level_country, n_reprint_authorships = sum([weight] * is_reprint_author)
into #country_reprint_author_email_provider
from #pub_author_country_email as a
join #country_results as b on a.country_id = b.country_id
join wos_2513..country as c on a.country_id = c.country_id
join [2025_email_analysis_domain_classification_validation] as d on a.domain_name = d.domain_name
where pub_year between 2020 and 2024
	and is_reprint_author = 1
	and is_institutional_domain = 0
group by c.country, d.company_lowest_level_country
order by n_reprint_authorships desc
--536

select a.*
from
(
	select *
	from
	(
		select a.country, a.company_lowest_level_country, perc_reprint_authorships = a.n_reprint_authorships / b.n_reprint_authorships
		from #country_reprint_author_email_provider as a
		join #country_reprint_author as b on a.country = b.country
		join #country_email_provider as c on a.company_lowest_level_country = c.company_lowest_level_country
	) as a
	pivot
	(
		sum(perc_reprint_authorships)
		for company_lowest_level_country in ([United States], [China], [Russia], [Brazil], [South Korea], [India], [Japan], [Italy], [Germany], [Poland], [France], [Ireland], [Taiwan], [Switzerland], [United Kingdom])
	) as b
) as a
join #country_reprint_author as b on a.country = b.country
order by b.n_reprint_authorships desc

-- Overall publication and authorship statistics (2020-2024).
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
	where pub_year between 2020 and 2024
) as a

-- Validation statistics.
select n_classified_domain_name = count(*),
	n_classified_domain_names_using_edu_suffix_rule = sum(case when edu_suffix is not null then 1 else 0 end)
from [2025_email_analysis_domain_classification_validation]
--49290	38851

select n_pubs_with_email = count(*),
	n_pubs_with_email_and_classified_domain = sum(case when b.ut is not null then 1 else 0 end),
	perc_pubs_with_email_and_classified_domain = cast(sum(case when b.ut is not null then 1 else 0 end) as float) / count(*)
from
(
	select ut
	from [2025_email_analysis_pub_author_country_email]
	where email_id is not null
) as a
left join
(
	select a.ut
	from [2025_email_analysis_pub_author_country_email] as a
	join [2025_email_analysis_domain_classification_validation] as b on a.domain_name = b.domain_name
) as b on a.ut = b.ut
--165350566	163980838	0.991716218255945

select count(*),
	sum(case when is_institutional_domain_name_after_validation = 1 then 1 else 0 end),
	sum(case when is_institutional_domain_name_after_validation = 0 then 1 else 0 end),
	sum(case when is_institutional_domain_name_after_validation is null then 1 else 0 end),
	cast(sum(case when is_institutional_domain_name_after_validation is null then 1 else 0 end) as float) / count(*),
	cast(sum(case when is_institutional_domain_name_after_validation is null then n_emails else 0 end) as float) / sum(n_emails)
from [2025_email_analysis_domain_classification_validation]
--11608, 10838, 606, 164, 0.0141281, 0.00249826512951634
--49290, 48425, 651, 214, 0.0043416514505985, 0.00281269997222623

-- Assigned email address statistics.
select count(*),
	sum(case when is_institutional_domain is not null then 1 else 0 end),
	cast(sum(case when is_institutional_domain is not null then 1 else 0 end) as float) / count(*)
from #email_domain_name_classification
--14726394, 13022581, 0.884302090518561
--19951961, 18021413, 0.903240187769012

select n_email_addresses = count(*),
	n_email_addresses_institutional = sum(case when is_institutional_domain = 1 then 1 else 0 end),
	n_email_addresses_noninstitutional = sum(case when is_institutional_domain = 0 then 1 else 0 end),
	n_email_addresses_unknown = sum(case when is_institutional_domain is null then 1 else 0 end),
	perc_email_addresses_institutional = cast(sum(case when is_institutional_domain = 1 then 1 else 0 end) as float) / count(*),
	perc_email_addresses_noninstitutional = cast(sum(case when is_institutional_domain = 0 then 1 else 0 end) as float) / count(*),
	perc_email_addresses_unknown = cast(sum(case when is_institutional_domain is null then 1 else 0 end) as float) / count(*)
from #email_domain_name_classification
--19951961	11581867	6439546	18021413	0.580487652316482	0.32275253545253	0.903240187769012


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
left join [2025_email_analysis_email] as b on a.email_id = b.email_id
left join [2025_email_analysis_domain] as c on b.full_domain = c.full_domain
left join [2025_email_analysis_domain_classification_validation] as d on c.domain_name = d.domain_name
where is_reprint_author = 1
	and a.email_id is not null
	and a.is_institutional_domain is null
	and pub_year between 2018 and 2022
order by newid()

-- All non-institutional email domains.
select a.domain_name, a.n_reprint_authorships, dominating_country = c.country, dominating_country_n_reprint_authorships = b.n_reprint_authorships
from
(
	select domain_name,
		n_pubs = count(distinct ut),
		n_reprint_authorships = sum([weight])
	from #pub_author_country_email
	where is_reprint_author = 1
		and is_institutional_domain = 0
	group by domain_name
) as a
join
(
	select domain_name,
		country_id,
		[filter] = row_number() over (partition by domain_name order by sum([weight]) desc, country_id),
		n_pubs = count(distinct ut),
		n_reprint_authorships = sum([weight])
	from #pub_author_country_email
	where country_id is not null
		and is_reprint_author = 1
		and is_institutional_domain = 0
	group by domain_name, country_id
) as b on a.domain_name = b.domain_name and b.[filter] = 1
left join wos_2513..country as c on b.country_id = c.country_id
order by a.n_reprint_authorships desc
