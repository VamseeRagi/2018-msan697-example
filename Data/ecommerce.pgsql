--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.4
-- Dumped by pg_dump version 9.5.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: inventory; Type: TABLE; Schema: public; Owner: dwoodbridge
--

CREATE TABLE inventory (
    itemid integer,
    qty integer
);


ALTER TABLE inventory OWNER TO dwoodbridge;

--
-- Name: items; Type: TABLE; Schema: public; Owner: dwoodbridge
--

CREATE TABLE items (
    itemid integer,
    name character varying,
    shortdescription text,
    customerrating real,
    numreviews integer
);


ALTER TABLE items OWNER TO dwoodbridge;

--
-- Data for Name: inventory; Type: TABLE DATA; Schema: public; Owner: dwoodbridge
--

COPY inventory (itemid, qty) FROM stdin;
47919421	21
47919896	96
51624288	88
50732860	60
51057303	3
45990798	98
37327069	69
49760471	71
21682260	60
46790151	51
36202669	69
37655521	21
48809371	71
51049117	17
14660387	87
54149072	72
54149534	34
54149335	35
51057459	59
42382363	63
50647016	16
54148217	17
54148077	77
51060182	82
52755877	77
50647137	37
51060477	77
27942564	64
40851835	35
46905561	61
49561037	37
51058037	37
17130081	81
34120045	45
50646896	96
51059612	12
54148416	16
54047634	34
49561536	36
52758072	72
52756811	11
52757295	95
52756263	63
54146459	59
52757804	4
47826502	2
52756323	23
52756275	75
52740318	18
52755678	78
53992620	20
54148513	13
52757331	31
51057239	39
49561014	14
49561096	96
50645105	5
49560993	93
52740347	47
51057245	45
52755421	21
51088269	69
52756615	15
52509020	20
49650956	56
32802497	97
51213495	95
52756129	29
52755364	64
54149062	62
50645140	40
52508151	51
52507989	89
51057331	31
52508723	23
54147382	82
50647068	68
50646117	17
21100380	80
54149236	36
49560862	62
51057081	81
52757725	25
49560892	92
52740334	34
54148699	99
51057606	6
36202642	42
49561194	94
52802948	48
49368980	80
54288675	75
51057415	15
47826467	67
54288678	78
54288677	77
50163800	0
47187807	7
32802371	71
52507967	67
54065364	64
52508367	67
47463135	35
40378653	53
51267778	78
44680460	60
51232743	43
42423575	75
54288676	76
47919416	16
52508872	72
48809400	0
33930668	68
51640095	95
52507849	49
45698924	24
50911358	58
32802422	22
49667379	79
48042682	82
32106786	86
171603011	11
50880343	43
51232763	63
52241820	20
52241712	12
52241766	66
52241719	19
51230540	40
46905551	51
52400316	16
47349849	49
51231483	83
16934408	8
27254890	90
48809379	79
46905558	58
46905564	64
46905559	59
45701256	56
46789474	74
45699595	95
47424575	75
46905553	53
46905560	60
47826476	76
45993576	76
54024549	49
54022932	32
15627958	58
54501624	24
51010097	97
54024564	64
53407140	40
51033197	97
46789423	23
54272517	17
46905557	57
52668548	48
54058539	39
46905563	63
51054045	45
54024240	40
54023904	4
54020343	43
53993420	20
53985968	68
51838665	65
54021437	37
54025499	99
53993391	91
42423814	14
52890039	39
51745855	55
45031313	13
54025062	62
54025020	20
54024270	70
46905571	71
8071598	98
52078707	7
54024239	39
54023870	70
54025223	23
53993466	66
53407781	81
174847631	31
39099687	87
54501655	55
23031516	16
44376974	74
49969213	13
194992763	63
136226262	62
39099696	96
45912717	17
51746146	46
54307633	33
20575312	12
54058474	74
54058537	37
54023926	26
54025207	7
51009926	26
54000796	96
117943549	49
198279562	62
174593428	28
135751117	17
49781408	8
52668523	23
35600033	33
49689948	48
52162708	8
19501704	4
51033280	80
54025525	25
43777512	12
39099524	24
32983894	94
51031113	13
51023606	6
53407714	14
54307663	63
49909851	51
54272446	46
54272725	25
53407701	1
51031066	66
46699144	44
46905556	56
52757921	21
54058459	59
23750454	54
54025084	84
54024535	35
54020308	8
54021484	84
54022933	33
53993449	49
44377003	3
52078904	4
50880462	62
133059829	29
47835566	66
53452845	45
198503636	36
44377126	26
53430356	56
53700630	30
174601920	20
53407979	79
53450861	61
53588936	36
53422334	34
15847019	19
53407653	53
21650091	91
16828521	21
21124914	14
47839821	21
42519436	36
42423564	64
19524758	58
48629637	37
53443556	56
53453342	42
53453484	84
53427708	8
53407221	21
48464083	83
53700707	7
26956596	96
53700681	81
53458856	56
23031531	31
49541786	86
21678478	78
47746704	4
23146274	74
47741348	48
48981523	23
24697799	99
53700674	74
53407382	82
54159876	76
53997923	23
54000585	85
53994335	35
42124693	93
48227646	46
41167755	55
53997867	67
53997942	42
54000380	80
54000353	53
53998428	28
53426744	44
53426384	84
53407646	46
53459127	27
53426604	4
28504765	65
53993885	85
54000351	51
54000342	42
54000285	85
53994392	92
53997920	20
53994341	41
53997903	3
53994333	33
53994342	42
53993112	12
45983168	68
52242012	12
53700705	5
47110818	18
9189134	34
47377680	80
50770289	89
17510564	64
45701022	22
33071417	17
48042567	67
23081882	82
45697912	12
47741297	97
54155128	28
53993824	24
53997925	25
35138559	59
45698620	20
51103674	74
47839998	98
53700673	73
53700730	30
9189118	18
45042086	86
37565963	63
53407366	66
47424457	57
21693725	25
50793009	9
21676538	38
36171293	93
48629619	19
54000832	32
54000892	92
48226440	40
29339780	80
40845440	40
53700601	1
35118324	24
53427511	11
53433226	26
53441155	55
53466925	25
43777653	53
53318347	47
49056031	31
35138606	6
40084458	58
45925455	55
32802443	43
17371223	23
22108606	6
45983183	83
41000728	28
47026979	79
46950424	24
47840020	20
45656493	93
46954992	92
48304944	44
47026958	58
45698215	15
43225294	94
48221464	64
41000735	35
22295005	5
101203559	59
40852513	13
48218572	72
22295066	66
45698891	91
47027157	57
46632062	62
40171513	13
48226430	30
48227327	27
46470729	29
45983147	47
46486857	57
43777684	84
36859252	52
44680429	29
45699776	76
30307456	56
48633079	79
45983214	14
43777558	58
21805750	50
48812323	23
42519218	18
16934838	38
123778313	13
22281138	38
48936885	85
49418870	70
51267714	14
20963060	60
21805730	30
43921980	80
47395372	72
44908271	71
45640620	20
51254914	14
20575893	93
15127533	33
46950412	12
45983335	35
48221870	70
46470708	8
40372244	44
41474785	85
48629682	82
14669411	11
41000729	29
48994578	78
29339978	78
23146144	44
22096203	3
42137733	33
51441193	93
42671199	99
47027011	11
47471745	45
40854296	96
40607521	21
29655663	63
41154413	13
46074968	68
29339481	81
45983240	40
45640581	81
47208387	87
19409718	18
48981580	80
20626166	66
40851841	41
169658189	89
41474799	99
40849938	38
44827737	37
47506394	94
47505948	48
40401101	1
15549752	52
45698860	60
111641395	95
45997573	73
47829195	95
19252039	39
40848917	17
17481161	61
42519320	20
45641030	30
19255468	68
46579740	40
14586148	48
52654422	22
46391861	61
17509817	17
22401505	5
36907648	48
51806054	54
51439902	2
21633032	32
24519160	60
48303823	23
35925348	48
41859543	43
14586206	6
47919412	12
16641965	65
45498242	42
40850936	36
23657861	61
40848627	27
40379423	23
48629785	85
180753512	12
41475115	15
47750486	86
46699117	17
27375152	52
20959332	32
33078288	88
29655596	96
16327203	3
14091528	28
136579926	26
40844494	94
47472643	43
103951250	50
17182228	28
40851664	64
47489593	93
51805973	73
45640813	13
45640928	28
33705833	33
108209334	34
14899285	85
40374665	65
45698751	51
19529832	32
22032303	3
105965266	66
37565950	50
15690921	21
166437872	72
50893253	53
40084485	85
48963634	34
50238035	35
21665130	30
48994598	98
41859582	82
44826587	87
41475114	14
16561135	35
19227878	78
20925572	72
45641035	35
45640922	22
52654834	34
22141493	93
25074782	82
15542000	0
121702534	34
40373335	35
40854218	18
54148080	80
52547570	70
47828266	66
40848578	78
22357016	16
52496543	43
45641110	10
47332898	98
142802047	47
40852381	81
20607392	92
44160975	75
149827700	0
113509775	75
35308184	84
52678827	27
41154655	55
104405258	58
40373665	65
45699814	14
47829191	91
40849853	53
149755202	2
21270475	75
156672168	68
51719949	49
47829476	76
51675306	6
20822913	13
197812619	19
169502155	55
45697959	59
181805574	74
21177112	12
25169622	22
23842766	66
163060788	88
37421348	48
14669370	70
160828697	97
46691652	52
22227350	50
47187808	8
45983322	22
52497297	97
22295006	6
45622319	19
156126125	25
17133562	62
40850426	26
35358523	23
46302383	83
35342542	42
52495048	48
45698546	46
20509919	19
40845660	60
22088801	1
21685111	11
45698285	85
21667397	97
44299593	93
34931743	43
46075024	24
116220907	7
41155950	50
49163475	75
24258883	83
44829801	1
45698515	15
148079023	23
45701055	55
51231444	44
44724810	10
19264701	1
44174505	5
43865409	9
20626394	94
45920221	21
52660510	10
45698256	56
50905068	68
39552615	15
24017422	22
35117857	57
39418442	42
15574445	45
35118665	65
23423166	66
45698522	22
44728317	17
43805492	92
47829087	87
44829359	59
45983108	8
19528528	28
29655866	66
46007571	71
41567809	9
52660177	77
35841934	34
29655911	11
195873371	71
45701027	27
35757176	76
46415498	98
45983270	70
17371871	71
14654250	50
42643491	91
23129365	65
49584432	32
21779896	96
45983093	93
17118430	30
17754217	17
42202556	56
45919986	86
45640791	91
17480851	51
40391817	17
17371341	41
47828221	21
20463191	91
51046988	88
17480866	66
39475293	93
40236340	40
21554334	34
24904890	90
21876378	78
35151274	74
19593336	36
198385917	17
17506272	72
45701196	96
19500685	85
48633313	13
38225442	42
14919971	71
21369780	80
47828106	6
48322768	68
156645695	95
45700887	87
186004280	80
48322333	33
35175320	20
45699753	53
51805771	71
40382387	87
22108273	73
44299546	46
22281048	48
26639376	76
16643351	51
23085214	14
21266779	79
29372894	94
46355066	66
197464620	20
189844490	90
42620507	7
46302338	38
44773711	11
35117707	7
52497152	52
136437403	3
179217678	78
135959493	93
40562623	23
165961495	95
49537547	47
40853807	7
169568220	20
199439032	32
162123789	89
35907771	71
23085961	61
47473188	88
16643665	65
45698858	58
53977925	25
45698223	23
48303835	35
35174726	26
119118657	57
158831482	82
104787134	34
45983281	81
48308093	93
42124646	46
42519081	81
42812074	74
48603994	94
53977962	62
35174943	43
52192107	7
45698195	95
188907988	88
45983358	58
48322504	4
35118615	15
172337233	33
162949481	81
113903330	30
43948788	88
40555242	42
199181766	66
112212044	44
40276132	32
111331249	49
114974469	69
138589739	39
197694652	52
193288903	3
45698126	26
23085861	61
176059236	36
198608005	5
53407514	14
194627275	75
42124694	94
40848805	5
44724612	12
21914977	77
50950627	27
35138661	61
50770310	10
162280204	4
42502819	19
133003117	17
45699557	57
48293045	45
50154363	63
196017419	19
53407686	86
46302365	65
35907821	21
53407917	17
47714290	90
14669331	31
35240528	28
110755193	93
163386440	40
47827966	66
159751584	84
46646062	62
135112807	7
48044169	69
121843559	59
136048061	61
53407804	4
44626009	9
35757317	17
115163208	8
156521461	61
21841359	59
21030473	73
109298952	52
148291348	48
44775661	61
161061148	48
153340575	75
122669018	18
52496413	13
197331981	81
158578755	55
132615354	54
33024501	1
159114762	62
53978179	79
53977978	78
116012697	97
53407752	52
52696778	78
47710130	30
15689353	53
119491659	59
122232674	74
52496826	26
40375436	36
107720970	70
183395645	45
40276006	6
107276695	95
181514033	33
114567265	65
50244622	22
42519851	51
155330481	81
125964852	52
180291786	86
179863893	93
129266302	2
45641024	24
52496778	78
45983390	90
16642179	79
198107505	5
43348007	7
19264582	82
23085764	64
50906620	20
28617075	75
130450675	75
163613668	68
47929545	45
37405217	17
24906548	48
52652810	10
136002976	76
46360248	48
19416803	3
48309637	37
21129702	2
103134196	96
43250211	11
187611100	0
40372298	98
145452575	75
50904187	87
46646383	83
42519593	93
23085407	7
185734569	69
102641256	56
35757294	94
22029932	32
170903470	70
22225807	7
50630357	57
38467907	7
38769489	89
109887487	87
52495081	81
46075174	74
22948711	11
40381865	65
143231283	83
51496082	82
107855508	8
23129491	91
162742207	7
52496396	96
194716046	46
101233515	15
110321582	82
43348037	37
52496814	14
183347488	88
52265063	63
47829113	13
45700897	97
100260848	48
42519281	81
51805809	9
50623173	73
189556593	93
194204259	59
35291639	39
24519186	86
46944613	13
43347736	36
45698123	23
41804608	8
128442346	46
50904002	2
115180966	66
48875884	84
45699577	77
100756148	48
50887801	1
33076518	18
52645614	14
42518964	64
107938491	91
118628450	50
47829136	36
52497221	21
46349504	4
100011830	30
41154750	50
155528380	80
156924587	87
142205157	57
183773921	21
132902367	67
50624246	46
52660556	56
50622284	84
161606245	45
39474729	29
109590996	96
130708701	1
49500044	44
101066641	41
51805853	53
21089170	70
45640563	63
112419962	62
50905784	84
180380393	93
160747273	73
40168354	54
165097683	83
50625128	28
155483579	79
120765556	56
183693690	90
183485647	47
139818762	62
165616249	49
52496628	28
50903153	53
187436725	25
176359523	23
194338956	56
53977867	67
126101940	40
52265270	70
109388520	20
127466135	35
47855611	11
45698101	1
42519707	7
187050373	73
123556405	5
116376778	78
163761615	15
186616181	81
42507916	16
42519016	16
142179646	46
181651360	60
21676529	29
16934596	96
50624971	71
118377065	65
156087164	64
51483139	39
44758913	13
41154768	68
50622533	33
22246082	82
20745904	4
165308108	8
17338982	82
167905566	66
48306578	78
52495840	40
39958794	94
141530103	3
42519132	32
37405511	11
\.


--
-- Data for Name: items; Type: TABLE DATA; Schema: public; Owner: dwoodbridge
--

COPY items (itemid, name, shortdescription, customerrating, numreviews) FROM stdin;
47919421	Fathead San Francisco Giants Teammate Logo	This Fathead Teammate is a high definition image made of tough high-grade vinyl that's tear and fade resistant, so you can be assured your eye catching Fathead will stay strong. And if the thought of moving your Teammate scares you, have no fear, because it uses a low-tack adhesive that can be easily moved and reused without any damage to your wall. Choose from hundreds of licensed sports and entertainment images.	\N	\N
47919896	Fathead San Francisco 49ers Teammate Logo	This Fathead Teammate is a high definition image made of tough high-grade vinyl that's tear and fade resistant, so you can be assured your eye catching Fathead will stay strong. And if the thought of moving your Teammate scares you, have no fear, because it uses a low-tack adhesive that can be easily moved and reused without any damage to your wall. Choose from hundreds of licensed sports and entertainment images.	\N	\N
51624288	NFL San Francisco 49ers Halftime Quad Chair by Rawlings	NULL	\N	\N
50732860	MLB Women's San Francisco Giants Short Sleeve Top	NULL	\N	\N
51057303	San Francisco Giants Womens Short Sleeve Graphic Tee	NULL	\N	\N
45990798	Big Men's NFL San Francisco 49Ers Short Sleeve Tee	Also available in men's sizes	5	\N
37327069	NFL San Francisco 49ers Twin/Full Bedding Comforter	Show support for your favorite NFL team with this San Francisco 49ers Full or Twin Bedding Comforter. It is designed to fit both twin and full beds and is made with 100 percent polyester fibers that provide softness, comfort and durability. This officially licensed NFL comforter makes a wonderful addition to any Niners fanatic's bedroom set. It measures 72&quot; x 86&quot; and is machine washable.	4.4000001	4
49760471	MLB San Francisco Giants  Revolver Cap / Hat  - Fan Favorite	Fan Favorite - MLB Revolver Cap, San Francisco Giants	4.5	\N
21682260	NFL San Francisco 49ers Sheet Set	Make your room announce, &quot;A true NFL fan lives AND sleeps here!&quot; Accentuate all your other team gear with an OFFICIAL NFL &quot;Anthem&quot; Twin Sheet Set by The Northwest Company. Whether game night or just another night for sleeping, the bold and large pillow logo coordinated with the repeating printed logo on white background adds an unmistakable bit of flair to your bedding ensemble. This 100 percent polyester, machine washable sheet set comes with one fitted sheet, one flat sheet and one standard pillowcase.	4.75	3
46790151	MLB  Boys' San Francisco Giants Team Short Sleeve Tee	Let your boy support his team with this MLB Boys' St. Louis Cardinals Team Short Sleeve Tee. This comfy tee features a front graphic of his favorite team, short sleeves, and tagless for added comfort. Gear up on the next game with this cool Tee.	\N	\N
36202669	NFL San Francisco 49ers Decorative Bath Collection - Shower Curtain	The Shower Curtain by The Northwest Company let's you show your team spirit even while you're in the shower! Our shower curtain is dyed to match your favorite football team's true colors. The richness of color boldly contrasts with the repeated design of your team's name and logo. Pair this with our Shower Curtain Rings for the complete look! Clean your act up and shower like a real fan!	5	2
37655521	NFL San Francisco 49ers Hand Towel	Dry your hands off like a true fan with this official NFL hand towel by The Northwest Co. You'll feel fresh and ready to go after using this fluffy hand towel that's gentle on your hands. This bathroom accessory fit for any college fan is dyed to match your team's true colors and features their team logo. The quick absorbency allows you to be on the go in an instant!	4.14300013	6
48809371	MLB  San Francisco Giants Forest Cap / Hat  - Fan Favorite	NULL	4.71400023	\N
51049117	San Francisco 49ers Bean Bag Toss	NULL	\N	\N
14660387	NFL - San Francisco 49ers Floor Mats - Set of 2	&lt;p&gt;These San Francisco 49ers Floor Mats are ideal for people who want to keep their vehicles protected. They are made to endure all types of weather, and they also conveniently fit a wide variety of cars, SUVs or trucks. The undersides are equipped with a skid-free material to keep the product in place. These 100-percent PVC floor mats are easy to clean and maintain. They are also the perfect way to showcase your team spirit for the San Francisco 49ers. They are well equipped to perform the function of keeping your space tidy and clean. The officially licensed design and logo of the San Francisco 49ers is neatly embossed on these durable vehicle floor mats. Available in a rich black color, this product is an ideal accessory for your car. These NFL floor mats come in a set of two.&lt;/p&gt;	4.69199991	25
54149072	NFL San Francisco 49ers Toddler Cheerleader Set	NULL	\N	\N
54149534	NFL San Francisco 49ers Toddler Short Sleeve Fashion Top	NULL	\N	\N
54149335	NFL San Francisco 49ers Toddler Long Sleeve Tee	NULL	\N	\N
51057459	San Francisco Giants Womens Short Sleeve Graphic Tee	NULL	\N	\N
42382363	NFL Micro Raschel Deep Slant 50" x 60" Throw, San Francisco 49ers	Proudly show off your favorite NFL team with these eye-catching Deep Slant Micro Raschel Throws by The Northwest. These officially licensed NFL throws are coordinated with your favorite football team's colors and logo. Prepare for ultimate relaxation while watching your team play; these super-plush throws are incredibly soft.	5	2
50647016	MLB Women's San Francisco Giants Tank Top and Shorts Set	NULL	\N	\N
54148217	NFL San Francisco 49ers Youth Short Sleeve Graphic Tee	NULL	\N	\N
54148077	NFL San Francisco 49ers Toddler Fleece Top	NULL	\N	\N
51060182	San Francisco Giants Girls Short Sleeve Graphic Tee	NULL	\N	\N
52755877	NFL San Francisco 49ers Baby Girls Short Sleeve Bodysuit Set, 3-Pack	NULL	\N	\N
50647137	MLB Men's San Francisco Giants Knit Jam Shorts	NULL	\N	\N
51060477	San Francisco Giants Girls Short Sleeve Graphic Tee	NULL	\N	\N
27942564	NFL San Francisco 49ers Bed in a Bag Complete Bedding Set	The officially licensed NFL San Francisco 49ers Bedding Set is a wonderful way to show your love for your team. The cozy set has the team colors and logo. This 49ers twin bedding set is wonderful for any sports fan. It is made of durable 100 percent polyester. It is warm and comfortable. It comes in twin, full and queen sizes. The twin set includes a comforter, flat sheet, fitted sheet and pillow case. The full and queen set has an extra pillow case.&lt;br&gt;	3.85700011	14
40851835	San Francisco 49ers 11"x15" Garden Flag	NULL	5	\N
46905561	San Francisco 49ers Table Cover	Set the scene with the San Francisco 49ers Table Cover which helps protect tabletops from spills and scratches while providing the perfect backdrop for party supplies and decorations that celebrate the 49ers team. The sturdy plastic cover measures 54 inches by 102 inches and it features an all-over print on a bright red background, including a gold center stripe bordered in white and the team's name and logo. Sold individually, the disposable table cover can be gathered up and tossed in the trash for the easiest clean-up ever &mdash; score!	\N	\N
49561037	MLB - Big Mens San Francisco Giants Short Sleeve Team Tee	ALSO AVAILABLE IN MEN'S SIZES	4	\N
51058037	San Francisco Giants Womens Short Sleeve Graphic Tee	NULL	\N	\N
101203559	NFL San Francisco 49ers "Old School" 50" x 60" Mink Sherpa	NULL	\N	\N
17130081	Men's MLB San Francisco Giants Team Tee	&lt;ul class=&quot;noindent&quot;&gt;&lt;li&gt;100% cotton&lt;/li&gt;&lt;li&gt;Crew neck&lt;/li&gt;&lt;li&gt;Long sleeves&lt;/li&gt;&lt;li&gt;Team graphic on front&lt;/li&gt;&lt;li&gt;Player name and number at back&lt;/li&gt;&lt;/ul&gt;	4.33300018	3
34120045	San Francisco Football "I Bleed Red and Gold, Go San Francisco" T-Shirt, Red	Celebrate your love of football with this 100 percent cotton T-shirt in your favorite team's colors!	4	5
50646896	MLB Women's San Francisco Giants Knit Capri Pants	NULL	\N	\N
51059612	San Francisco Giants Girls Short Sleeve Graphic Tee	NULL	\N	\N
54148416	NFL San Francisco 49ers Juniors Graphic Tee	NULL	\N	\N
54047634	NFL San Francisco 49ers Youth Lounge Pant	NULL	\N	\N
49561536	MLB - Mens San Francisco Giants Short Sleeve Team Tee	NULL	\N	\N
52758072	NFL San Francisco 49ers Baby Boys Team Blanket Sleeper	NULL	\N	\N
52756811	NFL San Francisco 49ers Baby Girls Long Sleeve Bodysuit Set, 2-Pack	NULL	\N	\N
52757295	NFL San Francisco 49ers Baby Boys Football Print Bodysuit	NULL	\N	\N
52756263	NFL San Francisco 49ers Baby Boys Bodysuit, Pant and Cap Outfit Set, 3-Piece	NULL	\N	\N
54146459	NFL San Francisco 49ers Youth Hooded Fleece Top	NULL	\N	\N
52757804	NFL San Francisco 49ers Baby Boys Team Uniform Footysuit with Cleats	NULL	\N	\N
47826502	NFL  San Francisco 49ers Basic Cap / Hat  - Fan Favorite	Fan Favorite - NFL Basic Cap, San Francisco 49ers	4.82600021	\N
52756323	NFL San Francisco 49ers Kids Printed Team Sleep Pant	NULL	\N	\N
52756275	NFL San Francisco 49ers Baby Boys Mesh Dazzle Bodysuit	NULL	\N	\N
52740318	NFL San Francisco 49ers Baby Boys Accessory Set, 2 Caps and 2 Booties, 4-Piece	NULL	\N	\N
52755678	NFL San Francisco 49ers Baby Girls Short Sleeve Heart Camo Bodysuit	NULL	\N	\N
53992620	NFL San Francisco 49ERS Women's Long Sleeve Crew Neck Tee	Long sleeve crew neck tee.	\N	\N
54148513	NFL San Francisco 49ers Toddler Short Sleeve Top and Pant Set	NULL	\N	\N
52757331	NFL San Francisco 49ers Baby Girls Bodysuit, Pant and Cap Outfit Set, 3-Piece	NULL	\N	\N
51057239	San Francisco Giants Womens Short Sleeve Burnout Graphic Tee	NULL	\N	\N
49561014	MLB - Mens San Francisco Giants Short Sleeve Team Tee	ALSO AVAILABLE IN BIG MEN'S SIZES	\N	\N
49561096	MLB - Big Mens San Francisco Giants Short Sleeve Team Tee	ALSO AVAILABLE IN MEN'S SIZES	\N	\N
50645105	MLB Big Mens San Francisco Giants Knit Jam Short, 2XL	NULL	\N	\N
49560993	MLB - Mens San Francisco Giants Short Sleeve Team Tee	ALSO AVAILABLE IN BIG MEN'S SIZES	\N	\N
52740347	NFL San Francisco 49ers Baby Girls Accessory Set, 2 Caps and 2 Booties, 4-Piece	NULL	\N	\N
51057245	San Francisco Giants Womens Short Sleeve White Graphic Tee	NULL	\N	\N
52755421	NFL San Francisco 49ers Baby Boys Short Sleeve Bodysuit Set, 3-Pack	NULL	\N	\N
51088269	MLB San Francisco Giants Big Men's Color Block Jersey	NULL	\N	\N
52756615	NFL San Francisco 49ers Baby Boys Long Sleeve Bodysuit Set, 2-Pack	NULL	\N	\N
52509020	NFL Big Men's San Francisco 49Ers Tek Patch Fleece Hoodie	NULL	\N	\N
49650956	MLB - Mens San Francisco Giants Short Sleeve Team Tee	ALSO AVAILABLE IN BIG MEN'S SIZES	\N	\N
32802497	NFL San Francisco 49ers Car Seat Cover	Make yourself an honorary team member and ride in style with a bucket seat, form fitting Official NFL Car Seat Cover by Northwest. The cover, which slips on easily over the car's seat and has back straps for a more secure fit, has the added benefit of soft padding along the seat and back for a more comfortable feel. The team's logo is large, with padded embroidery that makes a stunning statement to passersby even when you are not around.	3	1
51213495	MLB San Francisco Giants Men's Color Block Jersey	NULL	\N	\N
52756129	NFL San Francisco 49ers Baby Girls Bodysuit, Bib and Cap Outfit Set, 3-Piece	NULL	\N	\N
52755364	NFL San Francisco 49ers Baby Boys Bodysuit, Bib and Cap Outfit Set, 3-Piece	NULL	\N	\N
54149062	NFL San Francisco 49ers Juniors Long Sleeve Graphic Tee	NULL	\N	\N
50645140	MLB Big Mens San Francisco Giants Knit Sleep Pant, 2XL	NULL	\N	\N
52508151	NFL Men's San Francisco 49Ers Short Sleeve Tee	NULL	\N	\N
52507989	NFL Men's San Francisco 49Ers Tek Patch Fleece Hoodie	NULL	\N	\N
51057331	San Francisco Giants Girls Short Sleeve Graphic Tee	NULL	\N	\N
52508723	NFL Big Men's San Francisco 49Ers Short Sleeve Tee	NULL	\N	\N
54147382	NFL San Francisco 49ers Juniors Fleece Top	NULL	\N	\N
50647068	MLB Men's San Francisco Giants Knit Sleep Pants	NULL	\N	\N
50646117	MLB Women's San Francisco Giants Knit Sleep Pants	NULL	\N	\N
21100380	San Francisco 49ers Heavy Duty Vinyl Rear Seat Car Utility M	NULL	5	1
54149236	NFL San Francisco 49ers Girls Cheerleader Set	NULL	\N	\N
49560862	MLB - Mens San Francisco Giants Short Sleeve Synthetic Team Tee	This MLB Men's San Francisco Giants Synthetic Team Tee has short sleeves and a crew neck for a cool and comfortable fit. You can still look sharp while you show off your spirit in this top. This MLB tee has a bright graphic on the front for a touch of color.	\N	\N
51057081	San Francisco Giants Womens Short Sleeve Team Color Graphic Tee	NULL	\N	\N
52757725	NFL San Francisco 49ers Baby Boys Team Sleep 'N Play Outfit	NULL	\N	\N
49560892	MLB - Big Mens San Francisco Giants Buster Posey Short Sleeve Synthetic Player Tee	ALSO AVAILABLE IN MEN'S SIZES	\N	\N
52740334	NFL San Francisco 49ers Baby Accessory Set, 2 Bibs and 1 Burp Cloth, 3-Piece	NULL	\N	\N
54148699	NFL San Francisco 49ers Girls Fleece Top	NULL	\N	\N
51057606	San Francisco Giants Womens Graphic Tank	NULL	\N	\N
36202642	NFL San Francisco 49ers Decorative Bath Collection - 12pc Shower Hooks	What better way to hold up a shower curtain than with shower rings that show off your favorite college team? These Shower Hooks by The Northwest Company allow you to do just that! This fan-favorite bathroom accessory shows off your team pride! This is a subtle, yet bold way to show team spirit in your bathroom. Pair these rings with our Shower Curtain for the complete look!	\N	\N
49561194	MLB - Mens San Francisco Giants Buster Posey Short Sleeve Synthetic Player Tee	ALSO AVAILABLE IN BIG MEN'S SIZES	\N	\N
52802948	MLB San Francisco Giants Big Men's Mini Pique Short Sleeve Polo, 2XL	NULL	\N	\N
46905551	San Francisco 49ers Souvenir Cup	Give party guests a souvenir of their favorite football team that they can use for sipping frosty drinks or filing with snacks for the game. The San Francisco 49ers 22-ounce Plastic Cup is constructed of rigid plastic in bright red that features the team's logo in white, black and gold. Sold individually, this cup makes a great gift for a guest of honor or you can purchase multiple cups to fill with party favors for the younger crowd at the celebration.	\N	\N
52400316	MLB License Stainless Steel San Francisco Giants Dog Tag Logo Pendant, 22" Chain	NULL	5	\N
49368980	NFL San Francisco 49ers Tween Camp Chair	Bring sportsmanship and colorful seating to any room with this NFL San Francisco 49ers Tween Camp Chair. With a simple and sophisticated fan-favorite design, the sleek chair adds comfort to almost any decor. It is outfitted with a San Francisco 49ers logo on the seat, showcasing your devotion to the team. Its foldable design also makes it easy to transport and store when it is not in use. The camp chair is made from 100 percent polyester for effortless cleaning and care. Simply spot clean as needed. The NFL San Francisco 49ers Tween Camp Chair has a weight capacity of up to 200 pounds, allowing for added versatility and ease of use. The chair is an ideal addition to teen and tween bedrooms, guest rooms and entertainment areas. It's also a simple seating solution for dorm rooms and small living spaces. It comes pre-assembled for immediate use.	\N	\N
54288675	NFL Licensed TPU Lamination Waterproof Furniture Protector, Chair, San Francisco 49Ers	NULL	\N	\N
51057415	San Francisco Giants Womens Short Sleeve Graphic Tee	NULL	\N	\N
47826467	NFL  San Francisco 49ers Revolver Cap / Hat  - Fan Favorite	Fan Favorite - NFL Revolver Cap, San Francisco 49ers	4.5	\N
54288678	NFL Licensed TPU Lamination Waterproof Furniture Protector, Recliner, San Francisco 49Ers	NULL	\N	\N
54288677	NFL Licensed TPU Lamination Waterproof Furniture Protector, Sofa, San Francisco 49Ers	NULL	\N	\N
50163800	NFL San Francisco 49ers Collapsible Storage Toy Trunk	NULL	\N	\N
47187807	Forever Collectibles NFL Team Elf, San Francisco 49ers	This officially-licensed elf is sporting your favorite team's logo on his sweatshirt and a Santa hat for the season. Start a new tradition this year with your 2015 team elf!	\N	\N
32802371	NFL San Francisco 49ers 20" x 30" Round Edge Bath Rug	Made for a true NFL fan, the official NFL Round Edge Bath Rug by The Northwest Company is the perfect mat to place in your bathroom to make a proud statement about your favorite football team! These 100 percent nylon rugs are dyed to color-coordinate with your favorite team's true colors and logo. It has a non-skid backing that keeps it in place. Step out of the shower and feel like a true NFL fan with this 20-inch by 30-inch bath rug.	5	1
52507967	NFL Men's San Francisco 49Ers C Hyde 28 Player Tee	NULL	\N	\N
54065364	NFL San Francisco 49ers 20 oz Ultra Tumbler	NULL	\N	\N
52508367	NFL Big Men's San Francisco 49Ers C Hyde 28 Player Tee	NULL	\N	\N
47463135	NFL San Francisco 49ers Breakaway Beanie with Pom / Hat  - Fan Favorite	Fan Favorite - Breakaway Beanie with Pom, San Francisco 49ers	5	\N
40378653	NFL San Francisco 49ers Velour Seat Belt Pads	Ride in safety, comfort, and style with these officially-licensed seat belt pads.	5	\N
51267778	NFL San Francisco 49ers "Prestige" 60" x 80" Raschel Throw	NULL	\N	\N
44680460	NFL San Francisco 49ers 50" x 60" Fleece Throw	Cuddle up with your favorite NFL team while watching the game or simply lounging around. The Northwest Company's NFL &quot;Marquee&quot; Series Fleece Throw will keep you warm and cozy with its super-soft fabric. The officially licensed NFL Fleece Throw is the perfect football fan accessory. This official NFL blanket is color-coordinated to match your team's colors, making the throw's design bold and unmistakable. Even if you're not at the actual game cheering on your favorite football team, this 50-inch by 60-inch vibrant throw allows you to show your pride from the comfort of your own home!	4	1
51232743	NFL San Francisco 49ers Game Day Shade Sunglasses	NULL	\N	\N
42423575	MLB Men's San Francisco Giants Synthetic Tee	Short sleeve synthetic	\N	\N
54288676	NFL Licensed TPU Lamination Waterproof Furniture Protector, Love Seat, San Francisco 49Ers	NULL	\N	\N
47919416	Fathead San Francisco Giants Team Pack	This Fathead Teammate is a high definition image made of tough high-grade vinyl that's tear and fade resistant, so you can be assured your eye catching Fathead will stay strong. And if the thought of moving your Teammate scares you, have no fear, because it uses a low-tack adhesive that can be easily moved and reused without any damage to your wall. Choose from hundreds of licensed sports and entertainment images.	\N	\N
52508872	NFL Men's San Francisco 49Ers Synthetic Tee	NULL	\N	\N
48809400	MLB  San Francisco Giants Basic Cap / Hat  - Fan Favorite	NULL	4.82600021	\N
33930668	Radtke Sports rice-pro Jerry Rice Signed San Francisco 49ers Authentic Proline Helmet	NULL	\N	\N
51640095	NFL San Francisco 49er's "Drawback" 55" x 70" Silk Touch Throw	NULL	\N	\N
52507849	NFL Men's San Francisco 49Ers Long Sleeve Tee	NULL	\N	\N
45698924	NFL San Francisco 49ers Chrome Automobile Emblem	This officially licensed emblem can be applied to any vehicle or other hard surface with the included 3M Automotive Tape, and it can be removed without damaging paint or surface. UV Protected for durability.	5	\N
50911358	NFL Players Associate Colin Kaepernick 50" x 60" San Francisco 49ers Players High Definition Silk Touch Throw	NULL	\N	\N
32802422	NFL San Francisco 49ers Decorative Bath Collection Lotion/Soap Pump	Look no further for the perfect bathroom accessory for the true NFL fan! Northwest's official NFL Lotion Pumps show off your pride being that they are color-coordinated to your team and decorated with your team's name and logo. Your favoriteNFL team's name and logo will strike attention with its bold, vertical design that's hard to miss. Durable and spirited, our NFL official team spirited lotion pump is refillable and will last a long time!	\N	\N
49667379	NFL "Cut Out" 30" x 60" Beach Towel, San Francisco 49ers	Show off your favorite NFL team whether you're at the pool or the beach with this impressive official NFL beach towel by The Northwest Company. Designed around your favorite football team, this 30-inch by 60-inch towel has your team's logo and name front and center, laid out horizontally with bold colors coordinated to your team. It is made of 52 percent cotton and 48 percent polyester and is machine washable.	\N	\N
48042682	NFL San Francisco 49ers Tackle Buddy	This inflatable Tackle Buddy is made of durable vinyl. It's sand-weighted for bounce-back action. Each Tackle Buddy measures 24 inches by 40 inches when inflated. Get one in your favorite team's colors today!	4	\N
32106786	Party Animal NFL San Francisco Forty Niners Vintage Sign	This embossed metal sign has an authentic vintage look that makes it the perfect addition to any bedroom, office, bar or fan cave.	\N	\N
171603011	San Francisco Giants MLB Twin Comforter Bed in a Bag (Soft & Cozy) (64in x 86in)	NULL	\N	\N
50880343	Pets First NFL San Francisco 49ers Pet Collar	NULL	\N	\N
51232763	NFL San Francisco 49ers Beachfarer Sunglasses	NULL	\N	\N
52241820	NFL San Francisco 49ers Color Bling Emblem	NULL	\N	\N
52241712	NFL San Francisco 49ers Mini Team Gnome	NULL	\N	\N
52241766	NFL San Francisco 49ers Alternative Color Bling Emblem	NULL	\N	\N
52241719	NFL San Francisco 49ers Camo Lanyard	NULL	\N	\N
51230540	NFL San Francisco 49ers Silicone Rubber Bracelet Set	NULL	\N	\N
53407366	47 Brand San Francisco Giants MLB Amble Snapback Baseball Cap SIZE: ADJ	NULL	\N	\N
40852513	San Francisco 49ers Die-cut Decal - 8"x8" White	NULL	\N	\N
47349849	Swingline NFL San Francisco 49ers Stapler, 25 Sheets Capacity	NFL edition shows off your favorite NFL team. Durable die-cast base increases stability to ensure lasting performance. Full-size stapler holds full strip of 210 staples. Positive locking latch secures staple position for consistent use. Low-staple indicator prevents sudden depletion.	\N	\N
51231483	NFL San Francisco 49ers Fan Band Bracelet	NULL	\N	\N
16934408	San Francisco 49ers Heritage Banner	Commemorate the evolution of your favorite team with the San Francisco 49ers Heritage Banner.	\N	\N
27254890	San Francisco 49ers Zombie Figurine	These undead sports fans have risen with a vengeance and are terror-bent on getting revenge on rivals of old! Each zombie features torn garments that are adorned with the colors and logo of your favorite sports team.	4.5	2
48809379	MLB  San Francisco Giants Clean Up Cap / Hat  - Fan Favorite	NULL	4.5	\N
46905558	San Francisco 49ers Straws, 24-Pack	Make sure all your party guests are sipping pretty with the San Francisco 49ers Straws which are printed with the team's colors and logo for added game day excitement. The white paper straws have a gold stripe that spirals upward with small &quot;SF&quot; logos printed along the length, making them perfect to use with icy cold drinks served in the matching 20 ounce cups. Sold in packages of 24, the straws can be offered with refreshments or they can be arranged in centerpieces for added game day fun.	\N	\N
46905564	San Francisco 49ers Dessert Plates, 8-Pack	Devoted 49ers fans will love using the San Francisco 49ers Dessert Plates for munching on snacks or dessert servings at tailgate events or backyard game day barbecues. The durable seven-inch plates are bright red with a gold border around the edge with a print of the 49ers gold and white helmet and name in the center, plates that are sure to score extra points with guests. Sold in packages of eight, the disposable plates are easy to combine with cups and napkins from the officially licensed collection of 49ers party supplies for the ultimate fan experience.	\N	\N
46905559	San Francisco 49ers Flag Banner	Decorate the party scene and score extra points with football fans with the San Francisco 49ers Flag Banner. Triangular plastic flags in bright red are printed with alternating prints of the 49ers team helmet and name or the team logo, making the 12-foot banner the perfect accessory to hang along table edges, windows or from a tailgate tent. Sold individually, the decorative banner is great for expressing team spirit and it coordinates perfectly with tableware from the officially licensed collection of San Francisco 49ers party supplies.	\N	\N
45701256	NFL San Francisco 49ers Gel Air Freshener , 3-Pack	This licensed air freshener features popular paper construction, long lasting scent and a classic design. It's the perfect air freshener for your vehicle.	5	1
46789474	MLB Boys' San Francisco Giants H Pence #8 Jersey	Let your boy support his team with this MLB Boys' San Francisco Giants H Pence #8 Jersey. This comfy tee features a front graphic of his favorite team, short sleeves, and tagless for added comfort. Gear up on the next game with this cool Tee.	5	\N
45699595	NFL San Francisco 49ers Tissue Boxes	Show your team pride at home with this officially licensed tissue box.	4.66699982	4
47424575	Forever Collectibles NFL 2015 Santa Hat, San Francisco 49ers	Enjoy the Holidays with your favorite team! Show your team spirit with this officially-licensed Santa Hat. Each fun-for-all-ages Santa Hat has your favorite team's logo and colors.	\N	\N
46905553	San Francisco 49ers Cups, 8-Pack	Enthusiastic football fans will believe their beverages taste even better when served in the San Francisco 49ers 20-ounce Plastic Cups which feature the team's colors and logo. The clear plastic container has the &quot;49ers&quot; printed on one side and the team logo on the other, making it the ideal cup or snack holder for backyard barbecues, game day viewing or a tailgating event. Sold eight per package, the cups are easy to combine with matching tableware and decorations from the 49ers officially licensed party supply collection.	\N	\N
46905560	San Francisco 49ers Plates, 8-Pack	Football season is here and the best way to score points with fans is to serve game day foods on the officially licensed San Francisco 49ers Plates which feature the team's famous logo and colors. The sturdy nine-inch round plate showcases the 49ers oval shaped logo that contains the overlapping letters &quot;SF&quot; and it will easily hold tasty ribs, burgers and chips or pizza slices to eat while watching the game live or on television. Sold eight per package, the disposable plates are easy to pair with matching tableware from the collection or they can be mixed with solid gold or white party supplies for a fun presentation.	\N	\N
47826476	NFL  San Francisco 49ers Clean Up Cap / Hat  - Fan Favorite	Fan Favorite - NFL Clean Up Cap, San Francisco 49ers	4.5	\N
45993576	NFL Men's San Francisco 49Ers Montana Jersey	Support your favorite player or team in this NFL Men's San Francisco 49ers Montana Jersey. It is made of comfortable polyester fabric and features printed graphics with his name and number. This short sleeve jersey has a crew neck and a stylish retro look.	\N	\N
54024549	NFL San Francisco 49Ers Rectangular Game Time Platter	NULL	\N	\N
54022932	NFL San Francisco 49Ers Silicone Lid	NULL	\N	\N
15627958	NFL San Francisco 49ers 12-Pack Kooler Bag	The soft-sided, 12-pack Kooler Bag is perfect for a day on the beach or an afternoon tailgating. Each bag features a Tough Duct exterior and heat-sealed interior liner. A handy pocket on the side is perfect for storing golf tees, suntan lotion, car keys, you name it. Officially licensed by the League.	3	1
54501624	C & I Collectables NFL San Francisco 49ers Licensed 2016 Panini and Donruss Team set	NULL	\N	\N
51010097	Pets First MLB San Francisco Giants Hoodie Tee Shirt	NULL	\N	\N
54024564	NFL San Francisco 49Ers Home And Away Salt And Pepper Shaker	NULL	\N	\N
53407140	San Francisco Giants MLB Game 59FIFTY Fitted Baseball Cap	NULL	\N	\N
51033197	Pets First MLB San Francisco Giants Pet Harness with Hood, Small	NULL	\N	\N
46789423	MLB  Boys' San Francisco Giants  Alternate Team Short Sleeve Tee	Let your boy support his team with this MLB Boys' San Francisco Giants Alternate Team Short Sleeve Tee. This comfy tee features a front graphic of his favorite team, short sleeves, and tagless for added comfort. Gear up on the next game with this cool Tee.	\N	\N
54272517	C & I Collectables MLB 6" x 8" Brandon Crawford San Francisco Giants Two-Card Plaque	NULL	\N	\N
46905557	San Francisco 49ers Oval Platters, 8-Pack	Stack the wonderful San Francisco 49ers Oval Platters at the beginning of the buffet line or on the food table for barbecues and hear fans roar their approval. The sturdy paper plates measure 10 inches by 12 inches and the bright red background is decorated with the 49ers team logo, with a faded image of the logo behind the first. Sold eight per package, the plates make the ideal dish for holding heartier meals or they can be used as trays to pass out appetizers, dessert morsels or party favors to guests.	\N	\N
52668548	Masterpieces San Francisco Giants 100-Piece Puzzle	NULL	\N	\N
54058539	NFL Licensed Furniture Protector, Love Seat, San Francisco 49ers	NULL	\N	\N
46905563	San Francisco 49ers Napkins, 16-Pack	Keep fans' fingers and faces neat and tidy and have plenty of the San Francisco 49ers Napkins available for game day barbecues and tailgate pot lucks. The absorbent two-ply napkins measure 6.5 inches square and feature a bright red background that is decorated with the team's name and gold and white helmet, the perfect disposable party supply to score points with guests. Pair the napkins with matching plates and cups to create the ultimate football scene for 49ers enthusiasts.	\N	\N
51054045	NFL San Francisco 49Ers Youth Size Tailgate Chair from Coleman by Rawlings	NULL	\N	\N
54024240	MLB San Francisco Giants Color Change Tumbler	NULL	\N	\N
54023904	MLB San Francisco Giants 2-Pack Ceramic Pilsner	NULL	\N	\N
54020343	MLB San Francisco Giants 4-Pack Ceramic Coasters	NULL	\N	\N
53993420	C Collectables NFL San Francisco 49ers Licensed 2015 Score Team Set and Favorite Player Trading Card Pack Plus Storage Album	NULL	\N	\N
53985968	NFL Licensed Polyester Fill Relaxation Uneck Travel Pillow, San Francisco 49Ers	NULL	\N	\N
51838665	20 oz San Francisco Giants Colored Ultra Tumbler	NULL	\N	\N
54021437	NFL San Francisco 49Ers Ceramic Game Time Bowl	NULL	\N	\N
54025499	NFL San Francisco 49Ers Double Bling Tumbler	NULL	\N	\N
53993391	C Collectables NFL San Francisco 410ers 10 Different Licensed Trading Card Team Sets	NULL	\N	\N
42423814	MLB Men's San Francisco Giants Buster Posey Player Jersey	Player name and number jersey	\N	\N
52890039	Woodrow Gig Bag, San Francisco 49ers	NULL	\N	\N
51745855	Game Time MLB Men's San Francisco Giants Starter Series Watch	NULL	\N	\N
45031313	NFL Boys' San Francisco 49ers SyntheticHooded Fleece Top	Show team spirit when wearing the NFL Boys' San Francisco 49ers Synthetic Hooded Fleece Sweatshirt Top. It is made of polyester and features a kangaroo pocket in the front. This NFL team sweatshirt features a ribbed hem and wrist cuffs for a finished look.	\N	\N
54025062	MLB San Francisco Giants 4-Pack Ceramic Magnet	NULL	\N	\N
54025020	MLB San Francisco Giants Artisan Pilsner Glass	NULL	\N	\N
54024270	MLB San Francisco Giants Color Wall Tumbler	NULL	\N	\N
46905571	San Francisco 49ers Beverage Napkins, 16-Pack	49ers fans are going to cheer and shout when they see the San Francisco 49ers Beverage Napkins set out at game day barbecues, tailgate parties or a super fan gathering. The absorbent drink napkins measure five inches square and feature a bright red background printed with the iconic oval team emblem with the white overlapping &quot;SF&quot; lettering, a shadow image of the logo in the upper left and gold borders along two edges. Sold 16 per package, the spirited napkins are fun to arrange in stacks at the drink table to wrap around cocktails or they can be spread out on the dessert table for wiping messy fingers.	\N	\N
8071598	Homeward Bound: The Incredible Journey / Homeward Bound II: Lost In San Francisco (Double Feature)	NULL	4.80999994	57
52078707	San Francisco 49ers White Uniform NFL Action Figure Set	NULL	\N	\N
54024239	MLB San Francisco Giants 2-Pack Bottle Stopper	NULL	\N	\N
54023870	MLB San Francisco Giants 2-Pack Espresso Mug	NULL	\N	\N
54025223	MLB San Francisco Giants 8-Pack Neoprene Coasters	NULL	\N	\N
53993466	C Collectables MLB San Francisco Giants Licensed 2015 Topps Team Set and Favorite Player Trading Cards Plus Storage Album	NULL	\N	\N
53407781	47 Brand San Francisco Giants MLB Glimmer Snapback Baseball Cap SIZE: ADJ	NULL	\N	\N
174847631	San Francisco 49ers - Final Game at Candlestick Park - Blakeway Panoramas NFL Print with Deluxe Frame and Double Mat	NULL	\N	\N
39099687	MLB San Francisco Giants Men's poly polo shirt	SAN FRANCISCO GIANTS MLB POLY SYNTHETIC POLO SHIRT WITH LEFT CHEST TEAM LOGO	\N	\N
54501655	C & I Collectables NFL San Francisco 49ers 11 Different Licensed Trading Card Team sets	NULL	\N	\N
23031516	MLB Men's San Francisco Giants Tee, Buster Posey	Go team! The MLB men's short sleeve tee is for the real fan. Breathable thanks to the soft cotton so you can play and run like a champ or simply wear to the game.	4	1
44376974	NFL Boys' San Francisco 49ers Short Sleeve Poly Team Tee	Your child will be happy to show support for his favorite team wearing the NFL Boys' San Francisco 49ers Short Sleeve Poly Team Tee. This tee is made from a comfortable 100% soft fabric and a front graphic team name and logo. Wear over a hoodie for chillier nights or alone on warmer days!	\N	\N
49969213	OYO Sports 157-Piece Game Time Set 2.0, San Francisco Giants	This building block play set is a great gift for sports fans young and young at heart. The official team word mark and primary logos are printed on the field.	\N	\N
194992763	San Francisco 49ers Night Football - End Zone View, Blakeway Panoramas NFL Print with Select Frame and Single Mat	NULL	\N	\N
136226262	San Francisco 49ers - Final Game at Candlestick Park - Blakeway Panoramas NFL Print with Select Frame and Single Mat	NULL	\N	\N
39099696	MLB San Francisco Giants Men's Poly Button Down Jersey	SAN FRANCISCO GIANTS MLB POLY BUTTON DOWN JERSEY WITH FULL CHEST TEAM APPLIQUE WITH LOGO	\N	\N
45912717	MLB San Francisco Giants Men's short poly pinhole mesh Jersey, 2XL	Show your love for the game and your favorite team in this officially licensed MLB jersey! This baseball jersey features a poly pinhole mesh for breathability and premium embroidery with your favorite team's logo.	\N	\N
51746146	Game Time NFL Women's San Francisco 49ers Frost Series Watch	NULL	\N	\N
54307633	NFL Licensed Memory Foam U-Neck Travel Pillow, San Francisco 49ers	NULL	\N	\N
20575312	Big Men's MLB San Francisco Giants Team Tee	&lt;ul class=&quot;noindent&quot;&gt;&lt;li&gt;100% cotton&lt;/li&gt;&lt;li&gt;Crew neck&lt;/li&gt;&lt;li&gt;Short sleeves&lt;/li&gt;&lt;li&gt;Team graphic on front&lt;/li&gt;&lt;li&gt;Player name and number at back&lt;/li&gt;&lt;/ul&gt;	\N	\N
54058474	NFL Licensed Furniture Protector, Recliner, San Francisco 49ers	NULL	\N	\N
54058537	NFL Licensed Furniture Protector, Chair, San Francisco 49ers	NULL	\N	\N
54023926	MLB San Francisco Giants Rectangular Game Time Platter	NULL	\N	\N
54025207	MLB San Francisco Giants Ceramic Game Time Bowl	NULL	\N	\N
51009926	Pets First MLB San Francisco Giants Pet Bandana	NULL	\N	\N
54000796	C Collectables MLB 8x10 Buster Posey San Francisco Giants 3-Card Plaque	NULL	\N	\N
117943549	San Francisco 49ers - Inaugural Game at Levi's Stadium - Blakeway Panoramas NFL Print with Select Frame and Single Mat	NULL	\N	\N
198279562	San Francisco 49ers - Final Game at Candlestick Park - Blakeway Panoramas NFL Print with Standard Frame	NULL	\N	\N
174593428	San Francisco 49ers Night Football - End Zone View, Blakeway Panoramas NFL Print with Deluxe Frame and Double Mat	NULL	\N	\N
135751117	San Francisco 49ers Night Football - End Zone View, Blakeway Panoramas NFL Print with Standard Frame	NULL	\N	\N
50793009	Joe Montana & Dwight Clark Dual Signed San Francisco 49ers White Panel Football w/ "The Catch" Insc. with "The Play"	NULL	\N	\N
49781408	SkootZ Wristband, San Francisco Giants, Pack of 2	SkootZ Wristbands and fan gear are officially licensed. They are sweat resistant, stretchy and extremely comfortable to wear. Rubber bracelets and silicone bracelets are no competition to Skootz wristbands.	\N	\N
52668523	Masterpieces San Francisco 49Ers Shake N Score	NULL	\N	\N
35600033	Baby Fanatic Snuggle Bear, San Francisco 49ers	Baby Fanatic&rsquo;s officially licensed snuggle bear is the perfect snuggle buddy for all little fans. Made of soft brushed plush with a silky satin lining, the comforting plush bear allows for tactile stimulation and security and features an embroidered team logo.	\N	\N
49689948	SkootZ Wristband, San Francisco Giants	SkootZ Wristbands and fan gear are officially licensed. They are sweat resistant, stretchy and extremely comfortable to wear. Rubber bracelets and silicone bracelets are no competition to Skootz wristbands.	\N	\N
52162708	MLB License Stainless Steel San Francisco Logo Ring	NULL	\N	\N
19501704	San Francisco Giants Official MLB 3' X 5' Banner Flag by Wincraft	NULL	\N	\N
51033280	Pets First San Francisco Giants Pet Leash, Small	NULL	\N	\N
54025525	NFL San Francisco 49Ers 2-Pack Hype Travel Tumbler	NULL	\N	\N
43777512	Team Golf NFL San Francisco 49ers Golf Putter Grip	This multi-colored durable putter grip from Team Golf features your favorite team's logo. It includes a removable gel top ball marker with team logo.	\N	\N
39099524	MLB San Francisco Giants Big Men's Poly Button Down Jersey	SAN FRANCISCO GIANTS MLB POLY BUTTON DOWN JERSEY WITH FULL CHEST TEAM APPLIQUE WITH LOGO	\N	\N
32983894	Rico Industries San Francisco Giants Vinyl Grill Cover	Show your loyalty to your favorite team and cover your grill at the same time! Grill cover features your team logo and colors, plus it's made of durable vinyl to protect your grill from the elements. This grill cover can fit most large grills and the hook-and-loop Velcro closures keep things secure.	\N	\N
51031113	Pets First MLB San Francisco Giants Cat Collar	NULL	\N	\N
51023606	Pets First MLB San Francisco Giants Pet Pink Jersey, X-Small	NULL	\N	\N
53407714	47 Brand San Francisco Giants MLB Rockford Mesh Baseball Cap SIZE: ONE SIZE FITS MOST	NULL	\N	\N
54307663	NFL Licensed Logo Christmas Tree Skirt, San Francisco 49ers	NULL	\N	\N
49909851	NFL San Francisco 49ers Sky Glider Foam Airplane	NFL Officially licensed team glider. The 21.75-inch wingspan allows the glider to soar up to 75 feet. Comes in three easy-to-assemble sections. Team specific stickers are included. No tape or glue required. Made from EPO Foam.	\N	\N
54272446	C & I Collectables NFL 6" x 8" Navarro Bowman San Francisco 49ers Two-Card Plaque	NULL	\N	\N
54272725	C & I Collectables NFL 8" x 10" Navarro Bowman San Francisco 49ers 3-Card Plaque	NULL	\N	\N
53407701	San Francisco Giants MLB Clean Up Strapback Baseball Cap	NULL	\N	\N
51031066	Pets First MLB San Francisco Giants Nylon Baseball Rope Pet Toy	NULL	\N	\N
46699144	The Highland Mint MLB Man Cave Bronze Coin Panoramic Photo Mint, San Francisco Giants	Show your favorite team off in your man cave. This unique 12-inch by 20-inch frame with a 39mm minted commemorative coin is a must to decorate your wall! The Man Cave frame is featured in team-colored matting, and under a protective glass panel, a custom design in your team colors and logo as well as your 39mm minted team coin. Each Man Cave Photo Mint is officially licensed by the NFL.	\N	\N
46905556	San Francisco 49ers Bowls, 8-Pack	The wonderful San Francisco 49ers Bowls are the perfect disposable option for serving game day chili and soups or for filling with snacks to carry around the room while munching. The bright red paper bowls feature a picture of the 49ers gold and white helmet and name in the bottom to score extra points with football fans and they are the perfect match to additional tableware from the collection. Sold eight per package, the disposable bowls also make great containers for condiments, candies or relishes.	\N	\N
52757921	NFL San Francisco 49ers Baby Girls Mesh Dazzle Dress and Panty Set, 2-Piece	NULL	\N	\N
54058459	NFL Licensed Furniture Protector, Sofa, San Francisco 49ers	NULL	\N	\N
23750454	NFL Man Cave Banner, San Francisco 49ers	Show off your favorite sports team with this new Man Cave Banner that's designed to hang vertically. All of our banners are created with embroidery and appliqued with one of the highest quality wool blend felts. They measure eight inches by 32 inches and include a hanging cord.	\N	\N
54025084	MLB San Francisco Giants Stonewall Mug	NULL	\N	\N
54024535	NFL San Francisco 49Ers Ceramic Serving Plate	NULL	\N	\N
54020308	MLB San Francisco Giants 8-Pack Neoprene Car Coasters	NULL	\N	\N
54021484	NFL San Francisco 49Ers 8-Pack Neoprene Car Coasters	NULL	\N	\N
54022933	NFL San Francisco 49Ers Boxed Night Light	NULL	\N	\N
53993449	C Collectables MLB San Francisco Giants 4 Different Licensed Trading Card Team Sets	NULL	\N	\N
44377003	NFL Boys' San Francisco 49ers Team Hooded Fleece Top	Your child will be happy to show support for his favorite team wearing the NFL Boys' San Francisco 49ers Team Hooded Fleece Top. This hoodie is made from a comfortable 100% fabric and a front graphic team logo. Perfect for chillier game nights!	\N	\N
52078904	San Francisco 49ers Dark Uniform NFL Action Figure Set	NULL	\N	\N
50880462	Pets First MLB San Francisco Giants Pet Collar	NULL	\N	\N
133059829	San Francisco Giants MotiGlow	San Francisco Giants MotiGlow	\N	\N
47835566	MLB Forever Collectibles Reversible Split Logo Scarf, San Francisco Giants	Forever Collectibles - MLB Reversible Split Logo Scarf, San Francisco Giants	\N	\N
53452845	Picnic Time San Francisco 49ers PTX Backpack Cooler	NULL	\N	\N
198503636	San Francisco 49ers Deluxe Replica Speed Helmet	NULL	\N	\N
44377126	NFL Boys' San Francisco 49ers Long Sleeve Team Tee	Your child will be happy to show support for his favorite team wearing the NFL Boys' San Francisco 49ers Long Sleeve Team Tee. This tee is made from a comfortable 100% fabric and a front graphic team logo and name print. Perfect for chillier game nights!	\N	\N
53430356	47 Brand San Francisco Giants MLB Kirby Bucket Hat SIZE: ONE SIZE FITS MOST	NULL	\N	\N
53700630	C and I Collectables NFL 15W x 12H in. Steve Young San Francisco 49ers 8 Card Plaque	NULL	\N	\N
174601920	San Francisco 49ers - Inaugural Game at Levi's Stadium - Blakeway Panoramas NFL Print with Standard Frame	NULL	\N	\N
53407979	New Era San Francisco 49ers NFL 9Fifty Snapback Baseball Cap SIZE: ADJ	NULL	\N	\N
53450861	Legacy San Francisco 49ers Kickoff Serving Tray	NULL	\N	\N
53588936	San Francisco 49ers Official NFL Team Helmet Party Pack by Riddell 330273	NULL	\N	\N
53422334	Picnic Time San Francisco 49ers Bongo Cooler	NULL	\N	\N
47424457	Forever Collectibles NFL Sweater Piggy Bank, San Francisco 49ers	This officially-licensed Sweater Wearing Piggy Bank is a great way to save money for any sports fan! Each piggy bank is tailored to feature your team's colors, logos and theme!	\N	\N
21676538	San Francisco 49ers Official NFL 7 inch  Charm Bracelet by Wincraft	NULL	\N	\N
15847019	NFL Baby Fanatic 3-Piece Gift Set, San Francisco 49ers	Get your little one started on the road to fandom with this officially-licensed gift set from Baby Fanatic. It includes a bib, a bottle and a pacifier. Each item is BPA- and phthalate-free and is boldly decorated with colorful team graphics and colors. The pacifier and bottle feature a silicone nipple while the bib is 100 percent cotton with a Velcro closure and embroidered logo.	5	1
53407653	47 Brand San Francisco Giants MLB Woodside Stripe Snapback Baseball Cap SIZE: ADJ	NULL	\N	\N
21650091	San Francisco Giants Official MLB 12 inch x 6 inch  Plastic License Plate Frame by Wincraft	NULL	\N	\N
16828521	NFL San Francisco 49ers Wrap Sunglasses	These officially licensed sunglasses feature flex hinges for comfort and rubber sport grips. One side features the primary logo and the other the team name.	5	\N
21124914	Coleman Quad Chair with 4- to 6-Can Cooler, San Francisco 49ers	Enjoy relaxation in a chair with logos on the front and back featuring your own personal cooler.	5	2
47839821	NFL Forever Collectibles Reversible Split Logo Scarf, San Francisco 49e+BF11rs	Forever Collectibles - NFL Reversible Split Logo Scarf, San Francisco 49e+BF11rs	\N	\N
42519436	San Francisco 49ers Pennant	&lt;ul&gt;&lt;li&gt;Made of 100% Cotton, Woven.&lt;/li&gt;&lt;li&gt;12 X 30 Pennant.&lt;/li&gt;&lt;li&gt;Show your team pride in any room.&lt;/li&gt;&lt;li&gt;Officially licensed by the NFL and the San Francisco 49ers.&lt;/li&gt;&lt;/ul&gt;	\N	\N
42423564	MLB Big Men's San Francisco Giants Synthetic Tee	Short sleeve synthetic	\N	\N
19524758	San Francisco 49ers 27"x37" Banner	NULL	\N	\N
48629637	San Francisco 49ers Official NFL  ABS Helmet Ornament by Forever Collectibles	&lt;ul&gt;&lt;li&gt;Get the perfect decoration to let everyone know you are a huge fan!&lt;/li&gt;&lt;li&gt;This helmet ornament may be a small piece to add to your collection, but it'll make a big statement with its printed graphics.&lt;/li&gt;&lt;li&gt;Add this ornament to your array today and your team spirit will never take a backseat to any holiday!&lt;/li&gt;&lt;li&gt;Officially licensed by the San Francisco 49ers and the NFL.&lt;/li&gt;&lt;/ul&gt;	\N	\N
53443556	Picnic Time San Francisco 49ers PTX Backpack Cooler	NULL	\N	\N
53453342	Picnic Time San Francisco 49ers Zuma Cooler	NULL	\N	\N
53453484	Picnic Time San Francisco 49ers Zuma Cooler	NULL	\N	\N
53427708	Picnic Time San Francisco 49ers Topanga Cooler	NULL	\N	\N
53407221	American Needle San Francisco Giants MLB Tonalism Strapback Baseball Cap SIZE: ADJ	NULL	\N	\N
48464083	NFL San Francisco 49ers Car Flag, Style 2	Show your team pride on the go with this officially licensed car flag! It's made of 100 percent polyester, is printed on both sides and is UV fade-resistant.	\N	\N
53700707	C and I Collectables MLB 15W x 12H in. San Francisco Giants 2014 World Series Champions Plaque	NULL	\N	\N
26956596	Rawlings San Francisco 49ers Straight Leg Canopy, 10' x 10'	10' x 10' steel frameEasy two-person setupTeam logo and colored canopy	\N	\N
53700681	C and I Collectables MLB 15W x 12H in. San Francisco Giants 2012 World Series Champions Plaque	NULL	\N	\N
53458856	Picnic Time San Francisco 49ers Bongo Cooler	NULL	\N	\N
23031531	MLB Big Men's San Francisco Giants Tee, Buster Posey	Go team! The MLB men's short sleeve tee is for the real fan. Breathable thanks to the soft cotton so you can play and run like a champ or simply wear to the game.	4	1
49541786	NFL SF. 49ers Stripe One Necktie	Show off your true colors while staying professional. This tie is accented with team colored stripes and a woven logo on the front. Officially Licensed NFL Product.	\N	\N
21678478	San Francisco 49ers Official NFL 12 inch x 6 inch  Plastic License Plate Frame by Wincraft	NULL	5	1
47746704	Rawlings NFL Bleacher Cushion San Francisco 49ers	Sit in comfort with this padded San Francisco 49Ers team logo bleacher cushion. You can use it at the stadium, the office or in the car. It features integrated carry straps and a non-slip bottom.	\N	\N
23146274	San Francisco 49ers NFL Head Rest Cover	Accessorize your vehicle with Team ProMark's licensed Head Rest covers.	4	1
47741348	Boelter Brands NFL Gift Bucket Set, San Francisco 49ers	Show off your team spirit with this officially-licensed NFL pint and satin etched bucket set. This set is elegant, yet perfect for both personal use and to give as a gift! Each five-quart metal bucket comes with four 16 ounce pint glasses and four pulpboard coasters. Pints are dishwasher safe.	\N	\N
48981523	San Francisco 49ers Official NFL 4 inch  x 4 inch  Star Wars Darth Vader Die Cut Car Decal by Wincraft	NULL	\N	\N
24697799	San Francisco Giants Official MLB 12 inch x 6 inch  Metal License Plate Frame by Wincraft	NULL	\N	\N
53700674	C and I Collectables MLB 15W x 12H in. Buster Posey San Francisco Giants 8 Card Plaque	NULL	\N	\N
53407382	American Needle San Francisco Giants MLB Divided Snapback Baseball Cap SIZE: ADJ	NULL	\N	\N
54159876	San Francisco Giants Buster Posey 2013 Fathead Teammate	NULL	\N	\N
53997923	C Collectables MLB 12x15 Hunter Pence San Francisco Giants Player Plaque	NULL	\N	\N
54000585	C Collectables MLB 12x15 San Francisco Giants 2014 World Series Champions Plaque	NULL	\N	\N
53994335	C Collectables MLB 12x15 Buster Posey 2012 MVP San Francisco Giants Player Plaque	NULL	\N	\N
42124693	NFL Joe Montana San Francisco 49ers 11" x 14" Card Frame	This frame has one eight- by 10-inch photo that is double-matted.	\N	\N
48227646	San Francisco 49Ers NFL Adult Ugly Cardigan Sweater X-Large	NULL	\N	\N
41167755	San Francisco Giants 27"x37" Banner	NULL	\N	\N
53997867	C Collectables NFL San Francisco 49ers Licensed 2013 Score Team Set and Favorite Player Trading Card Pack Plus Storage Album	NULL	\N	\N
53997942	C Collectables MLB 16x20 San Francisco Giants 2010 and 2012 World Series Champions Plaque	NULL	\N	\N
54000380	C Collectables MLB 12x15 Madison Bumgarner 2014 World Series MVP San Francisco Giants Player Plaque	NULL	\N	\N
54000353	C Collectables NFL 12x15 Frank Gore San Francisco 49ers 8-Card Plaque	NULL	\N	\N
53998428	C Collectables NFL 12x15 Colin Kaepernick San Francisco 49ers 8-Card Plaque	NULL	\N	\N
53426744	Picnic Time San Francisco 49ers Tahoe Beach Bag	NULL	\N	\N
53426384	Picnic Time San Francisco 49ers Cart Cooler	NULL	\N	\N
53407646	American Needle San Francisco Giants MLB Back 2 Front Snapback Baseball Cap SIZE: ADJ	NULL	\N	\N
53459127	Picnic Time San Francisco 49ers Pranzo Lunch Cooler	NULL	\N	\N
53426604	Picnic Time San Francisco 49ers Pranzo Lunch Cooler	NULL	\N	\N
21693725	Wilson NFL Commemorative Championship 9" Football, San Francisco 49ers	Wilson's brings you a new series officially licensed footballs to show your pride for your favorite sports team. The nine-inch Commemorative Championship Football displays the Super Bowl logo for each year won, along with the team name and logo. Can be used for tossing around, bringing to the game or even decoration for all to see your dedication to the sport.	\N	\N
28504765	NFL San Francisco 49ers 42" x 13" Swooper Flag and 6' Pole	The NFL San Francisco 49er Flag is a vibrant and colorful way to display your team loyalty. Made of all-weather UV-resistant polyester, it can hold up against wind, rain and other weather elements. The pole is made of steel, making it durable and sturdy. When it catches the wind, the flag flies and displays the logo of the San Francisco 49ers NFL team. The flag and pole are lightweight and easy to transport so it can be set up on the lawn or walkway, It can also be used at pep rallies or tailgate events. This outdoor decorative flag measures 42&quot; x 13&quot; and includes a ground stake for keeping it firmly in place.	\N	\N
53993885	C Collectables MLB 12x15 Madison Bumgarner San Francisco Giants 8-Card Plaque	NULL	\N	\N
54000351	C Collectables NFL 12x15 Patrick Willis San Francisco 49ers 8-Card Plaque	NULL	\N	\N
54000342	C Collectables NFL 12x15 Steve Young San Francisco 49ers 8-Card Plaque	NULL	\N	\N
54000285	C Collectables MLB 12x15 Buster Posey San Francisco Giants 8-Card Plaque	NULL	\N	\N
53994392	C Collectables NFL San Francisco 49ers Licensed 2012 Score Team Set and Favorite Player Trading Card Pack Plus Storage Album	NULL	\N	\N
53997920	C Collectables MLB 12x15 Sergio Romo San Francisco Giants Player Plaque	NULL	\N	\N
53994341	C Collectables NFL 16x20 San Francisco 49ers All-time Great Photo Plaque	NULL	\N	\N
53997903	C Collectables MLB San Francisco Giants 2013 Team Plaque	NULL	\N	\N
53994333	C Collectables NFL 12x15 Colin Kaepernick San Francisco 49ers Player Plaque	NULL	\N	\N
53994342	C Collectables MLB 12x15 San Francisco Giants 2012 World Series Champions Plaque	NULL	\N	\N
53993112	C Collectables NFL 12x15 Montana-Young-Kaepernick San Francisco 49ers Legacy Collection Plaque	NULL	\N	\N
45983168	MLB San Francisco Giants 16 oz Metallic Coffee Travel Mug	This 16-ounce travel tumbler with high-definition metallic graphics features a spill-resistant lid and is insulated to keep beverages hot or cold.	4	\N
52242012	MLB San Francisco Giants Rain Poncho	NULL	\N	\N
53700705	C and I Collectables MLB 15W x 12H in. Madison Bumgarner 2014 World Series MVP San Francisco Giants Player Plaque	NULL	\N	\N
47110818	Game Time NFL Men's San Francisco 49ers Starter Series Watch	NULL	\N	\N
9189134	San Francisco 49'ers Deluxe Grill Cover	Durable and weather resistant, this deluxe cover extends the life of your grill by protecting it from the elements.	4.19999981	5
47377680	Boelter Brand NFL 64-Ounce Amber Growler Glass, San Francisco 49ers	Brew your favorite hop and impress your friends when you bring it out on game day in Boelter Brands' Amber Growler. This Amber Glass jar holds 64 ounces of your favorite liquid and is decorated with your favorite team's logo. It comes with a removable white cap. The convenient handle makes it easy to hold and to pour. The Amber Growler is intended for adult use.	\N	\N
50770289	San Francisco Giants 2014 World Series Champions Single Glass Baseball Case W/ Authentic AT Park Stadium Dirt	NULL	\N	\N
17510564	Rawlings Signature Series Full-Size Football, San Francisco 49ers	Get the whole team or your favorite player's autograph on the Signature Series Full-Size Football.	4	2
45701022	NFL San Francisco 49ers Shield Reflector	Great for cars, trucks, lockers, doors, mailboxes and RVs anywhere. Makes a perfect gift for your favorite sports fan.	\N	\N
33071417	WinCraft San Francisco 49ers Keystrap	This Officially Licensed one-inch soft polyester lanyard key strap features a split ring. The loop is approximately six inches.	\N	\N
48042567	NFL San Francisco 49ers Team Magnet	This Team Logo Magnet features your favorite team's logo screen-printed with UV-protected ink for great quality. It's made of heavy-gauge vinyl and will cling to all metal surfaces.	\N	\N
23081882	NFL San Francisco 49ers LED Flashlight, Small	This sporty flashlight features nine super-bright LED bulbs that last for more than 100,000 hours. It's also super tough, water resistant, and includes a lanyard so you can hang it anywhere.	\N	\N
45697912	NFL San Francisco 49ers Fathead Helmet Wall Graphic	This officially licensed NFL Fathead Helmet Decal is made of tough high-grade vinyl that is tear- and fade-resistant. Transform your sports cave, basement, family room, TV room, bedroom, workout room, office or any other interior area with ease! The low-tack adhesive can be moved and reused without damage to your FatHead or wall.	\N	\N
47741297	Boelter Brands MLB Gift Bucket Set, San Francisco Giants	Show off your team spirit with this officially-licensed MLB pint and satin etched bucket set. This set is elegant, yet perfect for both personal use and to give as a gift! Each five-quart metal bucket comes with four 16 ounce pint glasses and four pulpboard coasters. Pints are dishwasher safe.	\N	\N
54155128	47 Brand San Francisco 49ers Kirby NFL Kirby Bucket Hat SIZE: ONE SIZE FITS MOST	NULL	\N	\N
53993824	C Collectables MLB San Francisco Giants Licensed 2016 Topps Team Set and Favorite Player Trading Cards Plus Storage Album	NULL	\N	\N
53997925	C Collectables MLB 12x15 Madison Bumgarner San Francisco Giants Player Plaque	NULL	\N	\N
35138559	NFL 16 oz San Francisco 49ers Glass Jar with Lid and Handle, 2pk	Show off your passion for your team with this glass jar with lid and handle.	\N	\N
45698620	NFL San Francisco 49ers Stainless Steel Tumbler	This tumbler features a spill-proof and plastic interior, with a wrap logo that spans the whole tumbler.	4	1
51103674	San Francisco Giants MLB Retro and Team Logo Authentic Baseballs Bundle 2 Pack	NULL	\N	\N
47839998	MLB Forever Collectibles Lodge Gloves, San Francisco Giants	Forever Collectibles - MLB Lodge Gloves, San Francisco Giants	\N	\N
53700673	C and I Collectables MLB 15W x 12H in. Buster Posey 2012 MVP San Francisco Giants Player Plaque	NULL	\N	\N
53700730	C and I Collectables MLB 15W x 12H in. Madison Bumgarner San Francisco Giants 8 Card Plaque	NULL	\N	\N
9189118	San Francisco Giants Deluxe Grill Cover	Durable and weather resistant, this deluxe cover extends the life of your grill by protecting it from the elements.	5	2
45042086	Wilson 11" Junior Throwback Football, San Francisco 49ers	This NFL officially licensed 11-inch Junior Throwback Football by Wilson is made of composite leather and features composite leather stitching and laser stamped NFL team logo. This football holds seven to nine pounds of air and its sturdy construction helps hold its shape. Distressed brown color and black stitching help this football stand out. It can be used for decoration or to toss around.	\N	\N
37565963	NFL Handwarmer, San Francisco 49ers	Warm your hands up just like the pros do on the field! The official NFL Handwarmer by The Northwest was included in the Super Bowl XLVIII Welcome Package at MetLife Stadium, which was given to over 85,000 attendees. During the Super Bowl, Victoria's Secret Angel, Erin Heatherton was featured in three different Northwest billboards surrounding MetLife Stadium with this very handwarmer!. Have your favorite team keep your hands warm during those chilly games at the stadium. The 100 percent water-resistant nylon shell handwarmer with cuffs will keep your hands warm with the special &quot;Silk Touch&quot; fabric that is ultra-soft to the touch.	\N	\N
36171293	NFL San Francisco 49ers Decorative Bath Collection - Fingertip Towel	Dry your hands off like a true fan with this Official NFL Fingertip Towel by The Northwest Company. You'll feel fresh and ready to go after using this fluffy hand towel that is gentle on your hands. Your favorite football team's logo is embroidered on this towel that is perfect decor for a fan's bathroom! The quick absorbency allows you to be on the go in an instant!	\N	\N
48629619	San Francisco 49ers Official NFL  Repeat Glass Ball Christmas Ornament by Forever Collectibles	&lt;ul&gt;&lt;li&gt;Cheer on your favorite team during the holidays with this repeat pattern glass ball ornament!&lt;/li&gt;&lt;li&gt;Decorate your tree in team spirit.&lt;/li&gt;&lt;li&gt;This ornament features painted repeat team graphics and comes ready to hang.&lt;/li&gt;&lt;li&gt;Officially licensed by the San Francisco 49ers and the NFL.&lt;/li&gt;&lt;/ul&gt;	\N	\N
54000832	C Collectables NFL San Francisco 49ers Licensed 2014 Score Team Set and Favorite Player Trading Card Pack Plus Storage Album	NULL	\N	\N
54000892	C Collectables NFL 12x15 NaVorro Bowman San Francisco 49ers Player Plaque	NULL	\N	\N
48226440	San Francisco 49Ers NFL Adult Ugly Cardigan Sweater Large	NULL	\N	\N
29339780	MLB San Francisco Giants Champions Deluxe Frame, 11x14	This frame is a must-have for any fan! Show off your pride to your family, friends and colleagues.	\N	\N
40845440	San Francisco Giants Official MLB 8 inch x 8 inch  Die Cut Car Decal by Wincraft	NULL	\N	\N
53700601	C Collectables MLB 12x15 Brandon Crawford San Francisco Giants Player Plaque	NULL	\N	\N
35118324	NFL San Francisco 49Ers Sandwich Container Set, 4pk	Show off your favorite NFL team with this officially licensed sandwich container set. Comes with four containers ideal for travel, school lunches and tailgating. Sized for the largest of sandwhiches and features a team-colored lid and logo.	\N	\N
53427511	Picnic Time San Francisco 49ers Topanga Cooler	NULL	\N	\N
53433226	Ddi San Francisco Giants Pocket Tissues Countertop Display	NULL	\N	\N
53441155	Fathead San Francisco Giants Teammates Logo	NULL	\N	\N
53466925	Legacy San Francisco 49ers Symphony Serving Tray	NULL	\N	\N
43777653	Team Golf NFL San Francisco 49ers Victory Golf Cart Bag	This bag is rich with features, including an integrated dual handle top, six location embroidery, five zippered pockets, reinforced large external putter well, padded strap with strap pouch, fleece-lined valuables pouch, cooler pocket, removable rain hood, umbrella holder and towel ring.	\N	\N
53318347	The Sports Vault NFL 5-Piece Kitchen Knife Set, San Francisco 49ers	NULL	\N	\N
49056031	San Francisco 49ers Official NFL 4 inch  x 4 inch  Star Wars R2-D2 and C-3PO Die Cut Car Decal by Wincraft	NULL	\N	\N
35138606	NFL 20 oz San Francisco 49ers Plastic Souvenir Cups, 8pk	Show off your passion for your team with this cup.	\N	\N
40084458	Little Earth - NFL Prospect Messenger Bag, San Francisco 49ers	NULL	\N	\N
45925455	San Francisco 49ers Official NFL 5 inch x 7 inch  Finger Football Game by Wincraft	NULL	\N	\N
32802443	NFL San Francisco 49ers Decorative Bath Collection Toothbrush Holder	Even your toothbrush can show off your NFL pride! Start and end your day like a true fan by grabbing your toothbrush from our official NFL Toothbrush Holder. Color-coordinated to your favorite teams colors, our NFL official toothbrush holder shows off your team in an unmistakable way. These durable holders show off your favorite football team's name and logo, making them the perfect bathroom accessory for the avid NFL fan.	\N	\N
17371223	NFL San Francisco 49ers Medallion Door Mat	Wow your guests with officially licensed doormats by Fanmats. Adorned with your favorite team's logo, these doormats make a statement while keeping dirt and mud from entering your home.	4	1
22108606	NFL San Francisco 49ers Starter Mat	Decorate your home or office with officially licensed area rugs by Fanmats. They're great for fund raisers, gifts, businesses or just because! Each rug is made out of 100 percent nylon carpet and non-skid recycled vinyl backing.	\N	\N
45983183	MLB San Francisco Giants Breakaway Team Lanyards	Make a fandom statement with your keys using Aminco International's Team Lanyard. A decked out lanyard with all the features, keys and wallets can be easily clipped on and the repeating team pattern allows you to carry a little team spirit with you all day every day.	\N	\N
41000728	MLB 12x15 Stat Plaque, San Francisco Giants Champions	Show your support for the sport you love with this MLB 12x15 Stat Plaque.	\N	\N
47026979	Boelter Brands MLB Set of Two 16 Ounce Game Day Pint Set, San Francisco Giants	Toast your team in style this season with Boelter Brands' Game Day Pint Set. These pints hold up to 16 ounces of liquid and are decorated with team color logo and wordmark. Perfect for all your game day gatherings!	\N	\N
46950424	Forever Collectibles NFL 2015 Basic Stocking, San Francisco 49ers	Enjoy the holidays with your favorite team! Show your team spirit with this officially licensed stocking. Each stocking has your favorite team's logo and colors; it's fun for all ages.	\N	\N
47840020	NFL Forever Collectibles Lodge Gloves, San Francisco 49Ers	Forever Collectibles - NFL Lodge Gloves, San Francisco 49Ers	\N	\N
45656493	San Francisco 49ers All Pro Little Fan Baby Bib	NULL	\N	\N
46954992	Forever Collectibles NFL Real Ugly Sweater Gnome, San Francisco 49Ers	A Gnome with a an Ugly Sweater to match yours! This sweater-wearing gnome features an ugly design that will stand out on game day or any day for that matter! Made of lightweight PCV this gnome stands approximately 11 inches high and is officially licensed.	\N	\N
48304944	Little Earth LTL-350902-49ER-RED-DK San Francisco 49ers NFL Color Sheen Tote - Dark Red	NULL	\N	\N
47026958	Boelter Brands NFL Set of Two 16 Ounce Spirit Glass Can Set, San Francisco 49ers	Bring spirit to your favorite drink with Boelter Brands' 16-ounce Spirit Glass Can Set. This set of two unique can shape glasses are decorated with a variety of colorful team logos, dates and wordmarks.	\N	\N
45698215	NFL San Francisco 49ers 3D Foam Logo	This officially licensed 3D Foam Logo features an adjustable Velcro strap on the back with holes for easy hanging. It measures approximately 18 inches on its longest side.	\N	\N
43225294	NFL San Francisco 49ers Lunch Tote	This officially-licensed Lunch Tote features heavy zippered top and mesh side pockets, an adjustable shoulder strap and carrying handle.	\N	\N
48221464	San Francisco 49Ers Busy Block NFL Ugly Sweater XX-Large	NULL	\N	\N
41000735	MLB 12x15 4-Card/Pennant Plaque, San Francisco Giants Champions	Show your support for the sport you love with this MLB 12x15 4-Card/Pennant Plaque.	\N	\N
22295005	Game Time NFL Men's San Francisco 49ers Beast Series Watch	This officially licensed NFL Beast Watch is a heavy-duty watch that displays your team's logo in full color on a sunray dial. It has a durable and sweat-resistant polyurethane strap with a sports buckle. The glass on the watch is scratch-resistant mineral crystal so you don't have to worry about this watch getting nicks or scratches. The Beast watch is water resistant up to 99 feet and backed by a limited lifetime warranty. The strap length is 7.8 inches - 9.25 inches. A SR626 battery is included.	\N	\N
48218572	Woodrow NFL Guitar Strap, San Francisco 49ers	NULL	\N	\N
22295066	San Francisco 49ers 2012 Helmet	Fatheads are top-quality, officially licensed wall graphics of all sports leagues and top entertainment properties.&nbsp;They are the ultimate expression of fan pride. Fatheads are high definition die-cut images in a range of sizes and prices.&nbsp;Fatheads feature thick high-grade vinyl that resists tears, rips and fading for outstanding three-dimensional, life-like clarity. You can move Fatheads from wall to wall and there is no loss of adhesion. There's no damage to your walls &mdash; just peel and place!	\N	\N
45698891	NFL San Francisco 49ers carabineer Lanyard Keychain	This officially licensed Lanyard Keychain features your team's colors and an attached silver carabineer.	\N	\N
47027157	Boelter Brands MLB Set of Two 16 Ounce Spirit Glass Can Set, San Francisco Giants	Bring spirit to your favorite drink with Boelter Brands' 16-ounce Spirit Glass Can Set. This set of two unique can shape glasses are decorated with a variety of colorful team logos, dates and wordmarks.	\N	\N
46632062	San Francisco 49ers Official NFL 20 inch  x 12 inch  x 4 inch  Stripe Core Backpack by Forever Collectibles	Forever Collectibles - NFL Stripe Backpack, San Francisco 49Ers	\N	\N
40171513	Little Earth - NFL Sheer Infinity Scarf, San Francisco 49ers	NULL	\N	\N
48226430	San Francisco 49Ers NFL Adult Ugly Cardigan Sweater Medium	NULL	\N	\N
48227327	San Francisco 49Ers Wordmark NFL Ugly Sweater X-Large	NULL	\N	\N
46470729	MLB San Francisco Giants "Script" 54" x 84" Sweatshirt Throw	Think of your favorite sweatshirt being able to fit over your entire body. This official MLB Sweatshirt Throw by The Northwest Company does just that &mdash; mimic the feeling and comfort of your favorite sweatshirt, only bigger and better. This 54-inch by 84-inch jersey throw matches your favorite team colors, so no one will ever question who you're rooting for! Bring it to baseball games or cuddle under it while watching the game on TV.	\N	\N
45983147	MLB San Francisco Giants Chrome Automobile Emblem	Chrome-finished plastic Shield emblem can be applied to a vehicle or any other hard surface with the Shieldmotive 3M tape included on the emblem. Can be removed without damaging paint or surface and is UV protected.	\N	\N
46486857	NFL San Francisco 49er's Soft and Cozy Bedding Comforter Set	Super comfy and soft, this official NFL Soft and Cozy Comforter Set, by The Northwest Company, will make you never want to get out of bed! The large team logo stands out from the solid background on this bold bedding fit for a true fan. This set features a soft twin comforter, printed sham(s), flat sheet, fitted sheet and pillowcase(s). The bright colors enhance this NFL bedding set, not letting anyone question what team you're rooting for!	\N	\N
43777684	Team Golf MLB San Francisco Giants Victory Golf Cart Bag	This bag is rich with features, including an integrated dual handle top, six location embroidery, five zippered pockets, reinforced large external putter well, padded strap with strap pouch, fleece-lined valuables pouch, cooler pocket, removable rain hood, umbrella holder and towel ring.	\N	\N
36859252	San Francisco 49ers Round Table	This Round San Francisco 49ers Table is a great choice for your next outdoor adventure. Bring it to a tailgate party, a picnic, a patio cook out or most anywhere else you might like to eat. Made of 600 denier polyester, this round picnic table is durable and withstands heavy use without a problem. It features a screen-printed logo and four cup holders to ensure your beverages won't spill. Conveniently, this item is easily collapsible and includes a handy carrying bag, so transporting it from place to place is hassle-free.	\N	\N
44680429	MLB San Francisco Giants 50" x 60" Fleece Throw	Show off your unmistakable support for your favorite MLB team while watching the game or simply lounging around. The Northwest Company's MLB &quot;Strength&quot; Series Fleece Throw will keep you warm and cozy with its super-soft fabric. With an eye-catching, distressed vintage collage pattern &mdash; color-coordinated to your favorite team &mdash; this official MLB blanket will be an amazing baseball fan accessory for you. Even if you're not at the actual game cheering, this 50-inch by 60-inch vibrant throw allows you to show your pride from the comfort of your own home!	\N	\N
45699776	NFL San Francisco 49ers Flex Laser Cut Rubber Keychain	This officially licensed keychain is made out of a pliable rubber like material. What a great way to show off your team pride!	5	1
30307456	San Francisco 49ers Official NFL Hair Twist Ponytail Holder Little Earth 089795	NULL	\N	\N
48633079	NFL San Francisco 49ers Colin Kaepernick HD Mesh Mask	Root for the your favorite player with this fun High-definition Mesh Hooded Mask! Each mask features a black, mesh hood.	\N	\N
45983214	MLB San Francisco Giants Adhesive Decal with Silver Rhinestone Bling	This is a flexible emblem with an adhesive backing for placement on hard surfaces. The emblem incorporates rhinestones and glitter to give an amazing bling look.	\N	\N
43777558	Team Golf MLB San Francisco Giants Golf Putter Grip	This multi-colored durable putter grip from Team Golf features your favorite team's logo. It includes a removable gel top ball marker with team logo.	\N	\N
21805750	MLB - San Francisco Giants Collapsible Can Koozie	If you're the ultimate fan then show off your unwavering team spirit with this San Francisco Giants Collapsible Can Koozie. Features neoprene construction and screen print team design.	\N	\N
48812323	Snowman Jingle Bell Orns Prepack by The San Francisco Music Box Company Multi-Colored	NULL	\N	\N
42519218	San Francisco Giants Official MLB 11 inch x 15 inch  Garden Flag by Wincraft	&lt;ul&gt;&lt;li&gt;Show your team pride in your garden or entryway.&lt;/li&gt;&lt;li&gt;Features the official team logo.&lt;/li&gt;&lt;li&gt;Measures 11&quot; x 15&quot;.&lt;/li&gt;&lt;li&gt;Perfect of the front yard or patio.&lt;/li&gt;&lt;li&gt;Durable polyester construction is machine washable.&lt;/li&gt;&lt;li&gt;Designed to hang vertically from a garden flag pole or inside as wall decor.&lt;/li&gt;&lt;li&gt;Fits most standard garden flag stands.&lt;/li&gt;&lt;li&gt;Hanging stand is NOT included.&lt;/li&gt;&lt;li&gt;Official team logos and colors.&lt;/li&gt;&lt;li&gt;Officially licensed by the league and the team.&lt;/li&gt;&lt;/ul&gt;	\N	\N
16934838	Team Gnome, San Francisco 49ers	Show off your team pride in your garden or around the house with this Team Gnome. This adorable little fellow is perfect for the garden, patio, desk, shelf or doorway. Each gnome is hand crafted and hand painted with raised team logos on the hat. Give this little gnome a home this year!	3.66700006	3
123778313	San Francisco 49ers Official NFL  1996-2008 Throwback Deluxe Replica Helmet by Riddell	NULL	\N	\N
22281138	NFL - San Francisco 49ers Grill Mat 26" x 42"	NULL	\N	\N
48936885	San Francisco 49ers Official NFL 3' x 5' Star Wars Darth Vader Banner Flag by Wincraft	NULL	\N	\N
49418870	San Francisco 49ers Official NFL 4 inch  x 4 inch  Star Wars Yoda Die Cut Car Decal by Wincraft	NULL	\N	\N
51267714	MLB San Francisco Giants "Strike" 60" x 80" Raschel Throw	NULL	\N	\N
105965266	San Francisco Giants Buster Posey Caricature Premium Pennant by Wincraft, Inc.	NULL	\N	\N
20963060	Party Animal SF Giants Applique Banner Flag	Party Animal's signature item, a 44&quot; x 28&quot; true two-sided house flag is made of weather-resistant 420 denier nylon and is computer-embroidered for a high quality alternative to screen printed flags. It has a sewn-in sleeve for easy hanging off of your house flagpole and built-in hang tabs for easy indoor hanging. It's the perfect way to show your team pride!	\N	\N
21805730	San Francisco 49ers Official NFL Kolder Kaddy Can Holder by Kolder 261242	NULL	\N	\N
43921980	NFL San Francisco 49ers Team Checkers	Classic game of Checkers where the king wears a San Francisco 49ers helmet! The checkerboard is a representation of a football field with bright vivid team colors. The team logo is also located on each of the end zones. The checker pieces are made of high-quality plastic with team logo sticker. Great fun for you and a friend!	\N	\N
47395372	San Francisco Giants Official MLB One Size Striped Rugby Scarf by Forever Collectibles	NULL	\N	\N
44908271	San Francisco Music Box Company - Tree Topped Snowman Water Globe Multi-Colored	NULL	\N	\N
45640620	San Francisco 49ers Official NFL  Retractable Badge Holder Key Chain Keychain by Wincraft	NULL	\N	\N
51254914	MLB San Francisco Giants "Structure" 46" x 60" Micro Raschel Throw	NULL	\N	\N
20575893	San Francisco 49ers Die-cut Decal - 8"x8" Color	NULL	\N	\N
15127533	MLB San Francisco Giants "Big Stick" 50" x 60" Sherpa Throw	NULL	\N	\N
46950412	Forever Collectibles MLB 2015 Basic Stocking, San Francisco Giants	Enjoy the holidays with your favorite team! Show your team spirit with this officially licensed stocking. Each stocking has your favorite team's logo and colors; it's fun for all ages.	\N	\N
45983335	MLB San Francisco Giants Team Color Gamewear Bracelet	Show off your favorite Major League Baseball team by wearing this stylish, officially licensed baseball bracelet from GameWear. Each bracelet is made from genuine baseball leather and real baseball stitches and is adorned with your favorite team's logo and colors. Bracelets are one-size-fits-all and have a unique elastic baseball bead closure.	4	\N
48221870	San Francisco 49Ers Busy Block NFL Ugly Sweater X-Large	NULL	\N	\N
46470708	NFL San Francisco 49ers "Script" 54" x 84" Sweatshirt Throw	Think of your favorite sweatshirt being able to fit over your entire body. This official NFL Sweatshirt Throw by The Northwest Company does just that &mdash; mimic the feeling and comfort of your favorite sweatshirt, only bigger and better. This 54-inch by 84-inch jersey throw matches your favorite team colors, so no one will ever question who you're rooting for! Bring it to football games or cuddle under it while watching the game on TV.	\N	\N
40372244	San Francisco 49ers NFL 3x5 Flag	NULL	\N	\N
41474785	MLB 15x35 Triple Frame, San Francisco Giants Champions	Show your support for the game you love with this MLB 15x35 Triple Frame.	\N	\N
48629682	San Francisco 49ers Official NFL 4 inch  x 2 inch  Metal License Plate Christmas Ornament by Forever Collectibles	short description is not available	\N	\N
14669411	NFL Embroidered Trifold, San Francisco 49ers	The perfect gift for any sports fan. This genuine cowhide leather wallet has a team logo embroidered with quality stitching. It comes packaged in a metal gift tin printed in league design. Wallet contains pockets for credit cards and flaps for pictures. It is part of a line of finely-crafted leather goods that Rico has carried for more than 65 years.	\N	\N
41000729	MLB 12x15 6-Card Plaque, San Francisco Giants Champions	Show your support for the sport you love with this MLB 12x15 6-Card Plaque.	\N	\N
48994578	San Francisco 49ers Official NFL 30 inch  Star Wars Darth Vader Premium Pennant by Wincraft	NULL	\N	\N
29339978	MLB San Francisco Giants Patch Frame, 12x18	All pieces are double-matted and installed in a ready-to-hang frame. It looks absolutely gorgeous in a home, business or office. Impress your friends, family, employees and colleagues with this hanging on your wall. A must have for all true fans!	\N	\N
23146144	San Francisco Giants MLB Head Rest Cover	Accessorize your vehicle with Team ProMark's licensed Head Rest covers.	\N	\N
22096203	Riddell 2012 World Series Champ Mini Replica Helmet San Francisco Giants	NULL	\N	\N
42137733	NFL San Francisco 49ers Football Team Fuzzy Dice	This set includes TWO dice for your car! Hang from your rearview mirror or anywhere!	\N	\N
51441193	Copa America Men's San Francisco Venue Short Sleeve Tee	NULL	\N	\N
42671199	San Francisco 49ers Official NFL Bottle Opener Money Clip by Siskiyou 289767	NULL	\N	\N
47027011	Boelter Brands MLB Set of Two 12 Ounce Wine Glass Set, San Francisco Giants	Sip your favorite wine while watching the game with Boelter Brands' 12-ounce Game Day Wine Glass Set. This set of two stemmed wine glasses features team logo and team name. Hand wash only. Intended for adult use.	4	\N
47471745	San Francisco Giants Official MLB 17 inch  Knit Christmas Santa Hat by Forever Collectibles	NULL	\N	\N
40854296	San Francisco 49ers Official NFL 10 inch x 4 inch  Mini Pennant by Wincraft	NULL	\N	\N
40607521	San Francisco Giants Official MLB 11 inch x 17 inch  Car Window Cling Decal by Wincraft	NULL	\N	\N
29655663	San Francisco 49ers NFL Embossed Leather Billfold	? Genuine cowhide leather? Embossed team logo? Metal gift tin	\N	\N
41154413	San Francisco Giants Official MLB 8 inch  Keystrap Key Chain Keychain by Wincraft	NULL	\N	\N
46074968	San Francisco Giants Official MLB 2.5 inch  x 4.25 inch  Felt Pennant Magnet by Wincraft	NULL	\N	\N
29339481	San Francisco Giants 6-Card Plaque, 13x20	All pieces installed in a ready-to-hang plaque. It looks absolutely gorgeous in a home, business or office. Impress your friends, family, employees and colleagues with this hanging on your wall. A must have for all true fans!	\N	\N
45983240	MLB San Francisco Giants Die-Cut Color Auto Emblem	Made out of light weight aluminum for durability. The emblem is embossed and printed in team colors for a dynamic look. Great on any vehicle or hard surface.	\N	\N
45640581	San Francisco 49ers Official NFL  Lanyard Bottle Opener Key Strap Key Chain Keychain by Wincraft	NULL	\N	\N
47208387	NFL San Francisco 49ers Decorative Bath Collection	NULL	4.4000001	\N
19409718	San Francisco Giants Official MLB 4 inch x 4 inch  Die Cut Car Decal by Wincraft	NULL	\N	\N
48981580	San Francisco 49ers Official NFL 8 inch  Star Wars Lanyard Bottle Opener Key Strap Key Chain Keychain by Wincraft	NULL	\N	\N
20626166	San Francisco Giants Official MLB 4 inch x 6 inch  Car Window Cling Decal by Wincraft	NULL	\N	\N
40851841	San Francisco Giants Official MLB 17 inch x 26 inch  Banner Flag by Wincraft	NULL	\N	\N
169658189	San Francisco 49ers NFL 1996-2008 Mini Helmet w/ Z2B Face Mask Riddell 977256	NULL	\N	\N
41474799	MLB 15x35 Triple Autographed Frame, Madison Bumgarner San Francisco Giants	Show your support for the game you love with this MLB 15x35 Triple Autographed Frame.	\N	\N
40849938	San Francisco 49ers Official NFL 2 inch  Key Chain Keychain by Wincraft	NULL	\N	\N
23085961	C Collectables NFL 12x15 Jerry Rice San Francisco 49ers Career Stat Plaque	NULL	\N	\N
44827737	Logo NFL San Francisco 49ers DD Picnic Caddy	This picnic caddy features a soft inner lining and an interior zippered pocket. It has a collapsible design for easy storage and stylish team colors.	5	\N
47506394	San Francisco 49ers Official NFL 4 inch  Foam Christmas Ball Ornament by Forever Collectibles	&lt;ul&gt;&lt;li&gt;Each ornament measures approx. 3&quot;-4&quot; diameter.&lt;/li&gt;&lt;li&gt;Pre-strung with silver hanging thread. No need for hooks.&lt;/li&gt;&lt;li&gt;Each ornament is 1/4in thick.&lt;/li&gt;&lt;li&gt;Lightweight and durable foam construction. These won't break if they fall off the tree.&lt;/li&gt;&lt;li&gt;Great for the bottom layer of the tree where animals and kids can knock it off without harm.&lt;/li&gt;&lt;li&gt;Hang them from the tree, the mantel, rear view mirror, or at the office. Suitable just about anywhere.&lt;/li&gt;&lt;li&gt;Official team colors and logos.&lt;/li&gt;&lt;li&gt;Officially licensed by the San Francisco 49ers and the NFL.&lt;/li&gt;&lt;/ul&gt;	\N	\N
47505948	San Francisco 49ers Official NFL 5.5 inch  Foam Ugly Sweater Christmas Ornament by Forever Collectibles	&lt;ul&gt;&lt;li&gt;Dress up your tree this holiday season with the warmth of team spirit by adorning it with this knit sweater ornament.&lt;/li&gt;&lt;li&gt;This 5 1/2&quot; ornament features an embroidered team logo and woven patterns that make it look just like the sweaters your grandma used to give you at the holidays!&lt;/li&gt;&lt;li&gt;It even features a miniature hanger in place of a hook for a fun and easy decorating experience.&lt;/li&gt;&lt;li&gt;Officially licensed by the San Francisco 49ers and the NFL.&lt;/li&gt;&lt;/ul&gt;	\N	\N
40401101	San Francisco 49ers Black License Plate Frame  Free Screw Caps with this Frame	NULL	\N	\N
15549752	San Francisco Giants MLB FanBand	NULL	\N	\N
45698860	NFL San Francisco 49ers Bottle Bank, 21-Inch	This officially licensed bottle bank features printed graphics and a removable cap.	\N	\N
111641395	San Francisco 49ers Official NFL Fan Bead Bracelet by Siskiyou 306419	NULL	\N	\N
45997573	San Francisco Giants Official MLB 6 inch  x 9 inch  Chrome Die Cut Car Magnet by Wincraft	NULL	\N	\N
47829195	San Francisco Giants Official MLB Jersey Magnet Set Forever Collectibles 088131	NULL	\N	\N
19252039	Game Time MLB Men's San Francisco Giants Veteran Series Watch, Black Velcro Strap	From teenagers to grown-ups, the Veteran Watch by Game Time is an excellent pick. It comes with a resilient dark-colored velcro strap, which will complement wrist sizes large and small. The recognized team symbol sticks out on this particular timepiece from the metallic colored dial. The stainless steel backing will ensure the watch will never corrode or rust and will definitely protect the insides of the watch. If you are a rather busy team fanatic, you are going to appreciate this watch since it is very robust and compact. The Veteran Series additionally comes enclosed in a transparent plastic gift box that can be easily wrapped and handed to the fan you adore. Regardless of whether you are a diehard fan yourself or are buying for someone else, the Veteran Watch is a great watch for a very affordable price.	\N	\N
40848917	San Francisco Giants Official MLB 8 inch  Lanyard Key Chain Keychain by Wincraft	NULL	\N	\N
17481161	SAN FRANCISCO 49ERS 8' WIDE OFFICIAL TAILGATE BANNER	This unique NFL 8' Banner includes grommets to hang on the wall, but is also made of tailgate tough 420 denier nylon. This applique and embroidered banner is perfect for the game!	\N	\N
42519320	San Francisco 49ers NFL Metal Tag License Plate (#1 Fan)	NULL	\N	\N
45641030	San Francisco 49ers Official NFL 4 inch  x 4 inch  Colin Kaepernick Perfect Cut Car Decal 2-Pack by Wincraft	NULL	\N	\N
19255468	Game Time MLB Women's San Francisco Giants Glitz Watch, Silver	A fantastic choice for the female sports fan,&Acirc;&nbsp;the Glitz Watch from Game Time has an all metal case with 50 crystal stones around the bezel. The crystals shine and glimmer to make this a stunning timepiece. The attractive and durable patent-leather strap has a stainless steel buckle. The crown and case back are also stainless steel and will ensure your watch never rusts or corrodes. The brass dial features the official team logo and colors. The watch runs on a Japanese quartz movement, so it will keep very accurate time. It is water resistant and comes with a limited lifetime warranty. The Glitz Watch is a great watch at a fantastic price. Any female sports fan can proudly display their team pride with this attractive, yet affordable timepiece.	2	1
46579740	NFL San Francisco 49ers Drawstring Backpack	This Backsack is a lightweight and durable bag, convenient to take along for different activities and carry your gear while sporting your favorite team. Officially licensed by the San Francisco 49ers and the NFL.	\N	\N
14586148	Game Time MLB Men's San Francisco Giants Victory Series Watch	The Game Time Victory Series watch features an officially licensed color logo on the dial with a black glossy background behind it and comes in a rugged metal case with a stainless steel caseback. The Victory's sleek looking metal band with black rubber inserts complement the overall beauty of this watch. The watch has the accuracy and reliability of a Japan Quartz movement. The Victory is built to last as it is shock/water resistant up to 99 feet. and comes with a limited lifetime warranty. This Victory Series watch comes in a gift box that is perfect for wrapping. This durable watch is a great way to display your team pride year round. Strap is 7 inches - 8.5 inches. Battery - SR626 included.	\N	\N
52654422	San Francisco Giants 50x60 Fleece Blanket - Strength Design by Northwest Company	NULL	\N	\N
46391861	San Francisco 49ers Official NFL 6 inch  Team Logo Magnet by Forever Collectibles	NULL	\N	\N
17509817	San Francisco Giants AT Park Harbor Mural 51-51258	NULL	\N	\N
22401505	MLB Heritage Banner, San Francisco Giants	Show off your favorite team with this vintage-style wool banner with embroidery and applique detail.	\N	\N
36907648	Game Time MLB Women's San Francisco Giants Pearl Series Watch	The Game Time Pearl Series&nbsp;watch is a watch that is sure to please the ladies, it features a stainless steel case that beautifully showcases the silver team logo on a white dial and its white bezel sparkles. The bracelet features 10 ceramic beads on the bracelet with a&nbsp;stainless steel cap-folded brass crown&nbsp;with a&nbsp;butterfly clasp.&nbsp;The Pearl watch has the accuracy and reliability of a Japan Quartz movement, and is water resistant to 99ft and as with all Game Time's watches it is covered with a limited lifetime warranty&nbsp;which will guarantee many great years of use! If you are or know a female fan that is looking for a logo watch with style this is the watch for her. SR626 battery is included.	\N	\N
51806054	SAN FRANCISCO GIANTS OFFICIAL LOGO GARDEN FLAG + STAND	NULL	\N	\N
51439902	Copa America Big Men's San Francisco Venue Short Sleeve Tee, 2XL	NULL	\N	\N
45698751	NFL San Francisco 49ers Stainless Steel BBQ Set	Grill out in style with this set of stainless steel tools, including a large spatula, fork and tongs. The spatula features a bottle opener. These tools feature a cast and enameled team emblem.	\N	\N
19529832	San Francisco 49ers Official NFL Wall Mounted Bottle Opener by Siskiyou 089121	NULL	\N	\N
22032303	San Francisco Giants Licensed MLB Bunting 21" x 57"	NULL	\N	\N
21633032	Game Time NFL Women's San Francisco 49ers Pearl Series Watch	The Game Time Pearl Series&nbsp;watch is a watch that is sure to please the ladies, it features a stainless steel case that beautifully showcases the silver team logo on a white dial and its white bezel sparkles. The bracelet features 10 ceramic beads on the bracelet with a&nbsp;stainless steel cap-folded brass crown&nbsp;with a&nbsp;butterfly clasp.&nbsp;The Pearl watch has the accuracy and reliability of a Japan Quartz movement, and is water resistant to 99ft and as with all Game Time's watches it is covered with a limited lifetime warranty&nbsp;which will guarantee many great years of use! If you are or know a female fan that is looking for a logo watch with style this is the watch for her. SR626 battery is included.	\N	\N
24519160	NFL San Francisco 49ers Seat Cover	Make your ride the pride of the team with this high-quality seat cover from Fremont Die. This soft polyester velour seat cover features a team color embroidered logo and fits all bucket seats. It is held in place with adjustable straps and skirt hook.	\N	\N
48303823	Little Earth LTL-351005-49ER-BLACK San Francisco 49ers NFL Sport Noir Sheen Tote	NULL	\N	\N
35925348	Little Earth - NFL Pashi Fan Scarf, San Francisco 49ers	NULL	\N	\N
41859543	MLB 20x24 Suede Spotlight Frame, Tim Lincecum San Francisco Giants	Show your support for the sport you love with this MLB 20x24 Suede Spotlight Frame.	\N	\N
14586206	Game Time NFL Men's San Francisco 49ers Victory Series Watch	The Game Time Victory Series watch features an officially licensed color logo on the dial with a black glossy background behind it and comes in a rugged metal case with a stainless steel caseback. The Victory's sleek looking metal band with black rubber inserts complement the overall beauty of this watch. The watch has the accuracy and reliability of a Japan Quartz movement. The Victory is built to last as it is shock/water resistant up to 99 feet. and comes with a limited lifetime warranty. This Victory Series watch comes in a gift box that is perfect for wrapping. This durable watch is a great way to display your team pride year round. Strap is 7 inches - 8.5 inches. Battery - SR626 included.	\N	\N
47919412	Fathead San Francisco Giants WS Champs Teammate Logo	This Fathead Teammate is a high definition image made of tough high-grade vinyl that's tear and fade resistant, so you can be assured your eye catching Fathead will stay strong. And if the thought of moving your Teammate scares you, have no fear, because it uses a low-tack adhesive that can be easily moved and reused without any damage to your wall. Choose from hundreds of licensed sports and entertainment images.	\N	\N
16641965	Baby Fanatic MLB 2-Pack Baby Pacifiers, San Francisco Giants	Soothe your little fan with these officially licensed pacifiers. These orthodontic pacifiers feature a silicone nipple with a plastic shield and grasping hook. The team logo is adorned on the &quot;button&quot; with team colors decorating the shield. All items have been quality and safety tested to be 100 percent BPA- and phthalate-free.	\N	\N
45498242	San Francisco 49ers Official NFL One Size Sport Utility Work Gloves by Wincraft	Comfort &amp; style come together in the best selling sport utility glove. Made of heavy jersey cotton and a rubber dot palm for a sure grip.Officially licensed by the Oregon Ducks and the NCAA.	\N	\N
40850936	San Francisco 49ers Official NFL 4 inch x 4 inch  Die Cut Car Decal by Wincraft	NULL	\N	\N
23657861	MLB - San Francisco Giants Big Logo Sweatshirt Pillow	NULL	\N	\N
40848627	San Francisco 49ers Official NFL 27 inch x 37 inch  Banner Flag by Wincraft	NULL	\N	\N
40379423	San Francisco 49ers Official NFL One Size Spirit Fingers Pom Gloves by Wincraft	NULL	\N	\N
48629785	San Francisco 49ers Official NFL  Sled Christmas Ornament by Forever Collectibles	NULL	\N	\N
180753512	San Francisco 49ers Frank Gore 11"x17" Multi-use Decal Sheet	NULL	\N	\N
41475115	MLB 12x18 Photo Stat Frame, San Francisco World Champions	Show your support for the game you love with this MLB 12x18 Photo Stat Frame.	\N	\N
47750486	San Francisco 49ers Official NFL  Striped Rugby Scarf by Forever Collectibles	NULL	\N	\N
46699117	The Highland Mint NFL Man Cave Bronze Coin Panoramic Photo Mint, San Francisco 49ers	Show your favorite team off in your man cave. This unique 12-inch by 20-inch frame with a 39mm minted commemorative coin is a must to decorate your wall! The Man Cave frame is featured in team-colored matting, and under a protective glass panel, a custom design in your team colors and logo as well as your 39mm minted team coin. Each Man Cave Photo Mint is officially licensed by the NFL.	\N	\N
27375152	Creative Sports FB-SF49ERS-Signature San Francisco 49ers Embroidered Logo Signature Series Football	NULL	5	1
20959332	NFL San Francisco 49ers Get a Grip	Let your favorite team get a grip on your device! It's a great way to keep your phone in place and show off your favorite team. Never wonder where your phone is again. This new, officially licensed product innovation utilizes two polymer grips that adhere to each other, leaving you hands-free.	\N	\N
33078288	NFL San Francisco 49ers 14-Ounce Sculpted Warm Up Mug	The sculpted coffee mug is decorated with your team's bright and colorful graphics. The team logo is on both sides and is slightly raised. The mug also features the team's name in between the logos. It holds 14 ounces of fluid and is microwave and dishwasher safe. It measures approximately four inches tall.	5	1
29655596	San Francisco Giants MLB Embossed Leather Billfold	? Genuine cowhide leather? Embossed team logo? Metal gift tin	\N	\N
16327203	San Francisco 49ers Official NFL Salt & Pepper Shakers by Siskiyou 086984	NULL	\N	\N
14091528	Little Earth DO NOT SET LIVE! MLB San Francisco Giants Quilted Wristlet	NULL	\N	\N
136579926	San Francisco 49ers NFL Fandana and Jersey Purse Set	NULL	\N	\N
40844494	MLB San Francisco Giants Retractable Badge Holder, Black	NULL	\N	\N
47472643	SF 49ers 2'x3' Bold Logo Banne	SF 49ers 2'x3' Bold Logo Banne	\N	\N
103951250	San Francisco 49ers Majestic NFL "Of Great Value" Men's Short Sleeve T-Shirt	NULL	\N	\N
17182228	MLB - Men's San Francisco Giants Embroidered Trifold Wallet	&lt;p&gt;This MLB San Francisco Giants Embroidered Men's Trifold Wallet makes a stylish accessory for a loyal fan. It is made of genuine cowhide leather and features an embroidered team logo in full color. This trifold leather wallet comes packaged in a metal gift tin.&lt;/p&gt;	\N	\N
40851664	San Francisco Giants 57" Windsock	NULL	\N	\N
47489593	San Francisco Giants Official MLB  Desk Clock by Wincraft	NULL	\N	\N
51805973	SAN FRANCISCO 49ERS LOGO PENNANT GARDEN FLAG + STAND	NULL	\N	\N
45640813	San Francisco Giants Official MLB 4 inch  x 4 inch  Buster Posey Perfect Cut Car Decal 2-Pack by Wincraft	NULL	\N	\N
45640928	San Francisco 49ers Colin Kaepernick 11"x17" Multi-use Decal	NULL	\N	\N
33705833	Little Earth LTL-351002-49ER-BLACK San Francisco 49ers NFL Sport Noir Hoodie Purse	NULL	\N	\N
108209334	San Francisco 49ers NFL Perf-ect Bowler	NULL	\N	\N
14899285	Little Earth NFL Quilted Wristlet	NULL	\N	\N
40374665	Little Earth LTL-300101-49ER San Francisco 49ers NFL Team Jersey Tote	NULL	\N	\N
37565950	MLB Handwarmer, San Francisco Giants	Warm your hands up just like the pros do on the field with the official MLB Handwarmer. Have your favorite team keep your hands warm during those chilly games at the stadium. The 100 percent water-resistant nylon shell handwarmer with cuffs will keep your hands warm with the special &quot;Silk Touch&quot; fabric that is ultra-soft to the touch.	\N	\N
15690921	San Francisco 49ers Official NFL  Insulated Coozie Bottle Cooler by Kolder	NULL	\N	\N
166437872	San Francisco Giants Metal License Plate Frame - Orange	Let everyone know who you're rooting for with this Color Chrome License Plate Frame from Rico Industries! This frame is officially licensed and easy to mount on just about any license plate. &iuml;&iquest;&frac12;&iuml;&iquest;&frac12;&iuml;&iquest;&frac12;&iuml;&iquest;&frac12;&iuml;&iquest;&frac12;&iuml;&iquest;&frac12;The frame measures at 12&iuml;&iquest;&frac12;&iuml;&iquest;&frac12;&iuml;&iquest;&frac12; x 6&iuml;&iquest;&frac12;&iuml;&iquest;&frac12;&iuml;&iquest;&frac12; with plastic team inserts at the top and bottom of the frame. It&iuml;&iquest;&frac12;&iuml;&iquest;&frac12;&iuml;&iquest;&frac12;s decorated in vibrant team colors and its Zinc Metal construction makes it resistant to the elements.	\N	\N
50893253	Joe Montana and Dwight Clark Dual Signed San Francisco 49ers Proline Helmet w/ the play insc	NULL	\N	\N
40084485	Little Earth - NFL Large Prospect Weekender Bag, San Francisco 49ers	NULL	\N	\N
48963634	San Francisco 49ers Official NFL 4 inch  x 4 inch  Star Wars Storm Trooper Die Cut Car Decal by Wincraft	NULL	\N	\N
50238035	San Francisco 49ers Michael Crabtree Pennant Base Bobble Head	NULL	\N	\N
21665130	San Francisco Giants Vintage Tote	NULL	\N	\N
48994598	San Francisco 49ers Official NFL 30 inch  Star Wars Yoda Premium Pennant by Wincraft	NULL	\N	\N
41859582	MLB 15x35 Three Photo Autographed Frame, Pablo Sandoval San Francisco Giants	Show your support for the sport you love with this MLB 15x35 Three Photo Autographed Frame.	\N	\N
44826587	Logo MLB San Francisco Giants Quad Chair	This chair features a team logo on the front and back. It includes a cupholder and a carrying bag. This chair supports up to 225 pounds.	\N	\N
41475114	MLB 12x18 Double Frame, San Francisco Giants Champions	Show your support for the game you love with this MLB 12x18 Double Frame.	\N	\N
16561135	San Francisco 49ers Thematic Gnome	Forever Collectibles has done it again. This 2011 Thematic Gnome is a great way to show off your love for your favorite team. This gnome is uniquely hand painted and features the team's logo. It's exclusively decorated in a mascot-shaped hat with complete team colors and is fashionable down to the shoes to stand apart from the rest. Each team gnome is crafted to hold an item or two. It's great for all ages and looks good anywhere, whether it be on your bookshelf, desk, or patio or in a garden or doorway.	5	2
19227878	San Francisco 49ers Official NFL 4 inch x 4 inch  Die Cut Car Decal by Wincraft	NULL	\N	\N
20925572	San Francisco 49ers 15" Waste Basket	NULL	\N	\N
45641035	San Francisco 49ers Official NFL 5 inch  x  9 inch  Car Magnet 2-Pack by Wincraft	&lt;ul&gt;&lt;li&gt;Die cut logo magnets are made of weather resistant materials.&lt;/li&gt;&lt;li&gt;The logos are cut within the 5.125&quot; x 9&quot; sheet.&lt;/li&gt;&lt;li&gt;The magnet is great for the back bumper, refrigerator, or tool box.&lt;/li&gt;&lt;li&gt;Made in the USA.&lt;/li&gt;&lt;li&gt;Officially licensed by the San Francisco 49ers and the NFL.&lt;/li&gt;&lt;/ul&gt;	\N	\N
45640922	San Francisco 49ers Official NFL 11 inch  x 11 inch  Car Magnet by Wincraft	NULL	\N	\N
52654834	San Francisco 49ers Vintage Tote	NULL	\N	\N
22141493	Fathead NFL San Francisco 49ers Jerry Rice Wall Mural	NULL	\N	\N
25074782	San Francisco 49ers Window Film	Customize your car and show your team spirit at the same time with this team color window cling. It is made of lightweight perforated vinyl, which allows for a clear, unobstructed view when looking out the window. The window film work clings to all glass surfaces, requires no trimming and is easy to install.	5	1
15542000	MLB &#045; San Francisco Giants Game Day Purse	NULL	\N	\N
121702534	San Francisco Giants Official MLB Bottle Opener Key Chain by Siskiyou 244438	NULL	\N	\N
40373335	San Francisco 49ers Official NFL Coffee Mug by Boelter Brands 105359	NULL	\N	\N
40854218	San Francisco Giants Official MLB 4 inch x 4 inch  Each Die Cut Car Decal 2-Pack by Wincraft	&lt;ul&gt;&lt;li&gt;These terrific official logo decals are perfect for windows or windshields.&lt;/li&gt;&lt;li&gt;Die cut decals stick to multiple surfaces and are removable and reusable.&lt;/li&gt;&lt;li&gt;Sticks easily to windows, metal surfaces and painted surfaces, including most walls.&lt;/li&gt;&lt;li&gt;These versatile decals can be used indoors or outdoors and will not leave a residue when removed.&lt;/li&gt;&lt;li&gt;Terrific size. Each measures 4&quot; wide.&lt;/li&gt;&lt;li&gt;Crisp graphics. Innovative printing techniques give these decals a sharp look.&lt;/li&gt;&lt;li&gt;Perfect for the kids room or dorm.&lt;/li&gt;&lt;li&gt;Great for the car, office, fridge, or garage.&lt;/li&gt;&lt;li&gt;Official team logo.&lt;/li&gt;&lt;li&gt;Officially licensed by the league and the team.&lt;/li&gt;&lt;/ul&gt;	\N	\N
54148080	NFL San Francisco 49ers Toddler Windsuit	NULL	\N	\N
52547570	Ricky Watters Signed San Francisco 49ers Mini Helmet	NULL	\N	\N
47828266	San Francisco 49ers Terrell Owens 2003 Pro Bowl Forever Coll	NULL	\N	\N
40848578	San Francisco Giants Official MLB 3 inch x 10 inch  Die Cut Car Decal by Wincraft	NULL	\N	\N
22357016	San Francisco Giants SAN FRANCISCO GIANTS CRYSTAL LEGEND BRACELET	NULL	\N	\N
52496543	San Francisco 49ers Official NFL Bottle Opener Key Ring by Siskiyou 017759	NULL	\N	\N
45641110	San Francisco Giants Official MLB  Lanyard Bottle Opener Key Strap Key Chain Keychain by Wincraft	NULL	\N	\N
47332898	San Francisco 49ers 2015 Knit Santa Hat	NULL	\N	\N
142802047	San Francisco 49ers 2014 Women's Color Block Infinity Scarf	NULL	\N	\N
40852381	San Francisco 49ers Official NFL 18 inch  Christmas Santa Hat by Forever Collectibles	NULL	\N	\N
20607392	San Francisco 49ers Official NFL 6 inch x 9 inch  Car Magnet by Wincraft	Die cut logo magnet made of weather resistant materials. The logo is cut to the maximum size with the 6.25&quot; x 9&quot; sheet. The magnet is great for the back bumper, refrigerator, or tool box. Made in the USA	\N	\N
44160975	San Francisco 49ers Official NFL 2 inch  Oval Key Chain Keychain by Rico Industries	A metal keychain is just a metal keychain unless it has a full color graphic of your favorite teams' logo on one side. Then it becomes an expression of your love for the team, that just so happens to also keep your keys in check. It's a win-win.Officially licensed by the NFL and the San Francisco 49ers.	5	\N
149827700	San Francisco 49ers NFL Star Wars Dark Side/Light Side Premium Pennant 2pc Set (12in. x 30in.)	NULL	\N	\N
113509775	San Francisco Giants MLB Perfect Cut Color Decal 4" x 4"	NULL	\N	\N
47473188	San Francisco Giants Official MLB 18 inch  x 13 inch  x 1 inch  Chalk Backsack Backpack by Concept One	NULL	\N	\N
35308184	San Francisco 49ers Men's Sports Watch - Spirit	Includes China battery. Water and shock resistant to 3 ATM. Stainless steel caseback. Officially licensed team logos and colors. Supple rubber sports strap with sports buckle. Composite case ratcheting sport elapsed time bezel. Silver time and fluorescent hands. Team color dial. Precision quartz China movement. Case plating: Nil. Black PC case. Black silicon strap and band. Limited lifetime warranty. Case size: 33 mm.. Case width: 8 mm.. Lug size: 14 mm.. Total usable length: 7 in. - 9 in.. Weight: 0.15 lbs..	\N	\N
52678827	San Francisco 49ers Official NFL 2.5 inch  x 4.25 inch  Felt Pennant Magnet by Wincraft	NULL	\N	\N
41154655	San Francisco Giants Official MLB 3 inch x 10 inch  Perfect Cut Car Decal by Wincraft	NULL	\N	\N
104405258	Ladies Colin Kaepernick San Francisco 49ers Jersey Draft Him Fashion Top	NULL	\N	\N
40373665	San Francisco Giants MLB Black License Plate Frame	NULL	\N	\N
45699814	NFL San Francisco 49ers Original Team Lanyard Keychain	This 24-inch Team Lanyard has a detachable key ring. The lanyard comes in team colors with team logos. Your favorite team can be displayed where ever you go.	\N	\N
47829191	NFL San Francisco 49Ers 2Pk No-Spill Tumbler	NULL	\N	\N
40849853	NFL San Francisco 49ers Colored Snap Baby Bib	NULL	\N	\N
149755202	NFL Backpack Cooler by Picnic Time - Zuma, San Francisco 49ers - Black	NULL	\N	\N
21270475	MLB &#045; San Francisco Giants Stumpy Mini Gnome	NULL	\N	\N
156672168	San Francisco 49ers Patrick Willis 11"x17" Multi-use Decal S	NULL	\N	\N
51719949	San Francisco 49ers Glitter Stripe Scarf	NULL	\N	\N
47829476	San Francisco Giants MLB Barry Bonds Jersey Magnet Forever Collectibles 070471	NULL	\N	\N
51675306	San Francisco 49ers Scarf - 2014 Slogan	NULL	\N	\N
20822913	Baby Bib - San Francisco Giants	NULL	\N	\N
197812619	San Francisco 49ers NFL Quilted Hobo	NULL	\N	\N
169502155	San Francisco Giants Round Chrome Wall Clock	NULL	\N	\N
45697959	NFL San Francisco 49ers 11 Piece Stationery Set	This Eleven-Piece Stationery Set features a fun full-color NFL design. It's great for school, work, and just about anywhere. A great gift to give your sports fans for their birthdays, school or work gift, and for the Holidays!	\N	\N
181805574	San Francisco 49ers NFL Vertical Flag (27"x37")	NULL	\N	\N
21177112	MLB San Francisco Giants Sparkle Lanyard	NULL	\N	\N
25169622	MLB Insulated Travel Tumbler, San Francisco Giants	This officially licensed Insulated Travel Tumbler features a full wrap insert highlighting the designs of your favorite team. The dual-walled construction keeps your hot liquids hot and your cold drinks cold for hours. The lid has a slide lock that will cut down on spills.	\N	\N
23842766	Baby Fanatic 2-Pack Baby Bibs MLB San Francisco Gi	NULL	\N	\N
163060788	San Francisco 49ers Majestic NFL "Reflective 2" S/S Men's Charcoal T-Shirt	NULL	\N	\N
37421348	Coleman MLB San Francisco Giants Quad Chair	Showcase your team pride with this sports-themed Coleman San Francisco Giants Quad Chair. It is constructed with a durable powder-coated steel frame and can support up to 225 lbs. This Coleman folding chair comes with a matching carrying bag. It also has a built-in mesh cup holder. This sturdy quad folding chair features the San Francisco Giants team name, logo and colors.	\N	\N
14669370	NFL Embroidered Billfold - San Francisco 49ers	Genuine cowhide leather wallet. Has embroidered team logo. Packed in metal gift tin.	\N	\N
160828697	San Francisco 49ers - Inaugural Game at Levi's Stadium - Blakeway Panoramas NFL Print with Deluxe Frame and Double Mat	NULL	\N	\N
46691652	Bleacher Creatures NFL 9" Rally Men Hoodie Bear, San Francisco 49ers	The teddy bear is an iconic children's toy and is often given to adults to signify love, congratulations or now &mdash; team pride! Celebrate your NFL Team with a nine-inch Rally Man Plush Bear. This plush bear is made of soft, light brown polyester fibers. His sweet brown eyes, stitched brown nose and smile are sure to bring joy to your little or big fan's face! Sitting bear sports a team colored hoodie sweatshirt with printed team logo. Bring him to the game, have him join you on the couch or cuddle with him at night, this nine-inch bear will have you snuggling up close to your favorite team.	\N	\N
22227350	NFL &#045; San Francisco 49ers Stumpy Mini Gnome	NULL	\N	\N
47187808	San Francisco Giants Official MLB  Ugly Sweater Christmas Mug by Forever Collectibles	NULL	\N	\N
45983322	MLB San Francisco Giants 8 oz Mini Baseball Helmet Snack Bowls	Awesome mini batting helmets are specially designed to hold snacks. Slightly leveled top allows them to rest on any flat surface. Perfect size for peanuts, pretzels, snack mix, candy or ice cream. Each hat holds eight ounces.	\N	\N
52497297	SAN FRANCISCO 49ERS OFFICIAL LOGO GARDEN FLAG + STAND	NULL	\N	\N
22295006	Game Time MLB Men's San Francisco Giants Beast Series Watch	This officially licensed MLB Beast Watch is a heavy-duty watch that displays your team's logo in full color on a sunray dial. It has a durable and sweat-resistant polyurethane strap with a sports buckle. The glass on the watch is scratch-resistant mineral crystal so you don't have to worry about this watch getting nicks or scratches. The Beast watch is water resistant up to 99 feet and backed by a limited lifetime warranty. The strap length is 7.8 inches - 9.25 inches. A SR626 battery is included.	\N	\N
45622319	MLB San Francisco Giants 18" Mini Baseball Bat	Get the perfect item to update your fan cave and let everyone know you are a fan! This Team mini bat features bold team graphics in print on the barrel. You won't need to swing this bat to hit a home run, putting it on display will make your team spirit stand out to anyone who enters your fan cave!	\N	\N
156126125	San Francisco 49ers NFL High End Diagonal Zipper Drawstring Backpack	NULL	\N	\N
17133562	Concept One NFL Lunch Box	NULL	\N	\N
40850426	San Francisco 49ers Jewelry Gift Set	NULL	\N	\N
35358523	NFL San Francisco 49ers Survivor Bracelet	Siskiyou FSUB075 San Francisco 49ers Survivor Bracelet	\N	\N
46302383	OYO Sports 64-Piece NFL End Zone Building Block Set, San Francisco 49ers	Set the stage for your favorite team and their opponents with the End Zone Set from OYO. The officially licensed OYO End Zone sets are the perfect way to display your favorite OYO players. Official team lettering is printed on the end zones. Each field set includes two OYO team character minifigures, one OYO referee minifigure, a football, a water bottle and the 59 OYO Building Blocks needed to build the End Zone for your team. With 64 pieces in the boxed set, this is a great gift for any fan. Printed and assembled in the USA, OYO End Zone sets are compatible with other notable building block toys such as Lego, K'nex, Mega Bloks and more.	\N	\N
42202556	Master Pieces San Francisco 49ers Panoramic Stadium Puzzle, 1,000 Pieces	Just like being in the stadium for the big game. This 1,000-piece, interlocking Panoramic NFL San Francisco 49ers stadium puzzle measures 13 inches by 39 inches when assembled, and is hours of challenging fun for the entire family. Officially licensed by the NFL.	\N	\N
16643665	San Francisco 49ers Official NFL Team Sunglasses by Siskiyou 002120	NULL	\N	\N
35342542	Sparo MLB Women's San Francisco Giants Charm Watch	Women now have a chance to show their team spirit in style with the Charm watch. The fashion foward piece has a team logo which includes; a silver tone inlay, a mother of pearl dial, and applied crystals. The Charm watch features a high-precision quartz movement, stainless steel caseback and a scratch-resistant mineral crystal lens. Its water and shock resistant to 3 ATM (99ft) and comes with a limited lifetime warranty.	4	1
52495048	San Francisco 49ers Official NFL Bottle Opener Tag Necklace by Siskiyou 456008	NULL	\N	\N
45698546	NFL San Francisco 49ers Ombre Lanyard	This new ombre lanyard comes with a clip and a bottle opener attachment.	\N	\N
20509919	NFL 3-Piece BBQ Set with Canvas Case, San Francisco 49ers	Our stainless steel three-piece BBQ tool set includes a large spatula with built-in bottle opener, heavy duty tongs and large fork. All the tools feature a team logo on the handle. The set comes with a durable canvas bag that has a chrome accented team logo. Perfect for tailgating or any backyard cookout! This BBQ set features all the tools you need for a successful cookout and is easy to transport coming with a convenient carrying case. You will be the envy of any party with your officially licensed BBQ tool set!	\N	\N
40845660	San Francisco 49ers 2014 NFL Big Logo Scarf	NULL	\N	\N
22088801	San Francisco 49ers Loud Mouth Mascot	NULL	\N	\N
21685111	San Francisco 49ers Dangle Earrings	NULL	\N	\N
45698285	NFL San Francisco 49ers Aviator Sunglasses	These officially licensed NFL sunglasses offer maximum UV protection. They have flex hinges for comfort, reflective lenses, and a silk-screened team logo.	\N	\N
21667397	Wilson NFL 9" Color Throwback Football, San Francisco 49ers	Whether you are in the yard or tailgating at the game, this football will be the perfect addition to your game-day fun.	\N	\N
44299593	San Francisco Giants MLB Chrome License Plate Frame	NULL	\N	\N
34931743	12oz MLB San Francisco Giants Game Time Jar	Show off your favorite MLB team with this uniquely crafted game time jar. Bring team spirit to the kitchen or tabletop with this sculpted ceramic jar that can hold your favorite candy or snacks. Officially licensed and displays a colorful team graphic.	1	1
46075024	San Francisco Giants Official MLB 500 pc 500 Pc Jigsaw Puzzle by Wincraft	NULL	\N	\N
116220907	San Francisco 49ers NFL Star Wars Yoda Premium Pennant (12in. x 30in.)	NULL	\N	\N
41155950	San Francisco Giants Official MLB 8 inch x 8 inch  Perfect Cut Car Decal by Wincraft	NULL	\N	\N
49163475	San Francisco 49er's Car Flag  - NFL Licensed #99705	NULL	\N	\N
24258883	Creative Sports Enterprises RDRSA-SF49ers San Francisco 49ers Riddell Speed Revolution Full Size Authentic Proline	NULL	\N	\N
44829801	Logo NFL San Francisco 49ers DD Jr Caddy	This picnic caddy features a soft inner lining and an interior zippered pocket. It has a collapsible design for easy storage and stylish team colors.	\N	\N
45698515	NFL San Francisco 49ers FanMug Helmet	FanMug keeps cold beverages cold and hot beverages hot! It also features a 13.5 oz removable cup which is dishwasher safe and BPA-free.	5	2
148079023	San Francisco 49ers Women's Majestic NFL "Roster Rush" 3/4 Sleeve T-shirt	NULL	\N	\N
45701055	NFL San Francisco 49ers Women's Designer Sunglasses with Rhinestones	These Women's Designer Sunglasses with Rhinestones feature flex hinges for comfort and offer maximum UVA/UVB protection.	\N	\N
51231444	NFL San Francisco 49ers Swirl Heart Shape Dangle Logo Earring Set	NULL	\N	\N
44724810	San Francisco Giants Official MLB 8 oz. Mini Baseball Helmet Ice Cream Snack Bowls (24) by Rawlings	NULL	\N	\N
19264701	San Francisco 49ers NFL Woven Tapestry Throw (Home Field Advantage) (48"x60")	NULL	\N	\N
44174505	Little Earth - NFL Prospect Backpack, San Francisco 49ers	NULL	\N	\N
43865409	NFL San Francisco 49ers Door Banner	Dress up your doors at home, office or at school with this attractive and durable door banner. Fits most standard doors, can work on both inside doors and outside doors.	\N	\N
20626394	San Francisco Giants Die-cut Decal - 8"x8" Color	&lt;ul&gt;&lt;li&gt;This terrific official logo decal is perfect for windows or windshields.&lt;/li&gt;&lt;li&gt;Die cut decals stick to multiple surfaces and are removable and reusable.&lt;/li&gt;&lt;li&gt;Sticks easily to windows, metal surfaces and painted surfaces, including most walls.&lt;/li&gt;&lt;li&gt;These versatile decals can be used indoors or outdoors and will not leave a residue when removed.&lt;/li&gt;&lt;li&gt;Terrific size. Measures 8&quot; wide.&lt;/li&gt;&lt;li&gt;Crisp graphics.&lt;/li&gt;&lt;li&gt;Innovative printing techniques give these decals a sharp look.&lt;/li&gt;&lt;li&gt;Perfect for the kids room or dorm.&lt;/li&gt;&lt;li&gt;Great for the car, office, fridge, or garage.&lt;/li&gt;&lt;li&gt;Official team logo.&lt;/li&gt;&lt;li&gt;Officially licensed by the league and the team.&lt;/li&gt;&lt;li&gt;A terrific gift idea.&lt;/li&gt;&lt;li&gt;Availability: In Stock and Shipping.&lt;/li&gt;&lt;/ul&gt;	\N	\N
45920221	San Francisco Giants 11-Ounce Ceramic Mug, 2-Pack	Now you and a friend can enjoy a mug of your favorite beverage with this set of two 11-ounce Ceramic Color Coffee Mugs. These officially licensed mugs are adorned with your favorite team logo.	\N	\N
52660510	San Francisco 49ers Official NFL Garden Flag by Party Animal 163155	NULL	\N	\N
45698256	NFL San Francisco 49ers 3D Sandwich Container	This officially licensed 3D Sandwich Container features holographic team graphics and a secure snap closure.	\N	\N
50905068	Colin Kaepernick San Francisco 49ers Autographed Red Jersey with Kaepernicking Inscription - Fanatics Authentic Certified	NULL	\N	\N
39552615	NFL San Francisco 49ers 6-Pack Tube Cooler	This insulated cooler is made of durable vinyl with an easy-carry strap. It carries a six-pack of your favorite beverage. It has has a clip to attach to your golf bag or cart.	1	\N
24017422	MLB - Money Clip and Cardholder, San Francisco Giants	This fine leather cash and cardholder is the perfect way to organize both your cash and cards while showing off your team spirit! Each cash and cardholder is made of high quality Napa Grain Leather and features an ID window, 2 credit card slots and money	\N	\N
35117857	16oz NFL San Francisco 49Ers Swirl Straw Tumbler	Show off your favorite NFL team with this officially licensed swirl straw tumbler. This BPA-free double walled tumbler is made of acrylic and comes equipped with your teams colorful logo and team-colored swirl straw. Use with your favorite liquid at home or on the go.	2.5	2
39418442	NFL - San Francisco 49ers Motorcycle Mat 82.5 x 42	NULL	\N	\N
15574445	NFL - Money Clip and Cardholder,  San Francisco 49ers	This fine leather cash and cardholder is the perfect way to organize both your cash and cards while showing off your team spirit! Each cash and cardholder is made of high quality Napa Grain Leather and features an ID window, 2 credit card slots and money	\N	\N
45698858	NFL San Francisco 49ers Bling iPhone 5/5S Applique	This officially licensed applique gives your phone an added layer of protection. Includes full adhesive backing for easy peel-and-stick-application.	\N	\N
35118665	MLB San Francisco Giants 3-Compartment Lunch Container, 2pk	This officially licensed 2-Pack MLB San Francisco Giants 3-Compartment Lunch Containers is specially designed to help you celebrate your team and keep your foods separated. Equipped with three separate compartments, it will keep everything the way you prepared it. These divided lunch containers feature a team-colored lid and logo and are ideal to carry with you for a tailgate party or simply to lunch at school or the office. It comes in a multiple pack, so you can share the joy of your team or bring more snacks for everyone. It is BPA-free, so your food will remain safe.	\N	\N
23423166	NFL San Francisco 49ers Tire Cover	This spare tire cover is made with heavy-gauge vinyl that has an elastic tie down for an easy fit. The high-quality team color logo shows your team loyalty.	\N	\N
45698522	NFL San Francisco 49ers Game Day Cups	These 18 ounce plastic cups feature bright team logos.	\N	\N
44728317	San Francisco Giants Official MLB 8 oz. Mini Baseball Helmet Ice Cream Snack Bowls (5) by Rawlings	NULL	\N	\N
43805492	Steiner Sports San Francisco Giants Dirt Pen with Authentic Dirt from AT and T Park.	This incredible San Francisco Giants Executive Pen with authentic dirt from AT&amp;T Park is a great gift for the sports fan in your life. It is a perfect practical way to show your fandom to your team and the recipient will love it for years to come. This ball point pen writes in black ink and also comes with a functional stylus at the top for your smartphone or tablet. This pen also features the team logo and colors. The dirt inside this pen is MLB Authenticated and comes complete with a MLB Hologram and Steiner Sports Certificate of Authenticity.	\N	\N
47829087	Team Promark San Francisco 49er's Color Team Emblem Color Team Emblem	&lt;ul&gt;&lt;li&gt;Made out of light weight aluminum for durability.&lt;/li&gt;&lt;li&gt;The emblem is embossed and printed in team colors for a dynamic look.&lt;/li&gt;&lt;li&gt;Great on any vehicle or hard surface.&lt;/li&gt;&lt;li&gt;Officially licensed by the San Francisco 49ers and the NFL.&lt;/li&gt;&lt;/ul&gt;	\N	\N
44829359	Logo NFL San Francisco 49ers 30-Can Cooler Tote	This cooler features a heat sealed, leak-proof liner. It has a 30 can capacity. It features a front zippered pocket.	5	\N
45983108	MLB San Francisco Giants 4" Baseball Bat Keychain	Attach your favorite team to your set of keys with the MLB four-inch baseball bat keychain. This keychain is natural in color and has a clear lacquer finish to protect your team's printed logo.	5	1
19528528	San Francisco Giants Official MLB Wall Mounted Bottle Opener by Siskiyou 005190	NULL	\N	\N
29655866	San Francisco Giants  Tri-Fold Wallet (Pecan Cowhide)	? Genuine cowhide leather? Embossed team logo? Metal gift tin	\N	\N
46007571	Tudor Games San Francisco 49ers Dark Uniform NFL Action Figure Set	One team of 11 collectible San Francisco 49ers NFL action figures in five classic football poses with authentic NFL team colors and logos.	\N	\N
41567809	NFL San Francisco 49ers Window Film	This Window Film is made of perforated vinyl so that you can see right through. Window films are easy to install and look great!	\N	\N
52660177	San Francisco 49ers Michael Crabtree Pennant Base Bobble Hea	NULL	\N	\N
35841934	Game Time MLB Women's San Francisco Giants Frost Series Watch	NULL	\N	\N
29655911	San Francisco 49ers NFL Tri-Fold Wallet (Pecan Cowhide)	This genuine leather (interior and exterior) trifold wallet has six credit card holders, ID protector, and billfold pocket. Features embossed team logo and beautiful gift tin.	\N	\N
195873371	New Era MLB League 2 Tone 9Forty San Francisco Giants Team Men's Hat, 10963103	NULL	\N	\N
45701027	NFL San Francisco 49ers Neoprene Sunglass Strap	This flexible neoprene sunglass strap features tubes that slide easily over both thin and wide arm glasses, and silk screened team graphics. The strap is 16 inches long.	\N	\N
35757176	NFL Sun Shelter by Picnic Time, Manta - San Francisco 49ers, Red	The Manta Sun Shelter is portable and compact. Designed to set up in seconds, this open-front tent can be used wherever you might need protection against the sun or wind and has a UPF 50+ rating. It also has a built-in floor to act as barrier to the sand, hot cement or wet grass. A zippered mesh window allows cross-ventilation. Bring the Manta to the beach, the park or the lake. You'll be glad you did!	\N	\N
46415498	MLB Mens 2012 Commemorative San Francisco Giants Cufflinks w Box PD-SFG12-SL	NULL	\N	\N
45983270	MLB San Francisco Giants 4" Quick Toss Softee Baseball	Pass the time before opening pitch with the Jarden Sports Softee Quick Toss Baseball. This officially licensed squishy ball with vibrant team graphics is perfect for around the home and is safe for all ages.	\N	\N
17371871	NFL Cutting Board by Picnic Time, Asiago - San Francisco 49ers	The Asiago is a folding cutting board with tools that is another Picnic Time original design. This compact fully-contained split-level cutting board is made of eco-friendly rubberwood. Lift up the top level of the board to reveal four brushed stainless steel cheese tools: a pointed-tipped cheese knife, cheese fork, cheese chisel knife and blunt nosed hard cheese knife. The tools are magnetically secured to a wooden strip that lifts up so you can close the cutting board and display the tools. Designed with convenience in mind, the Asiago is great for home or anywhere the party takes you.	\N	\N
14654250	San Francisco 49ers NFL Laser Cut License Plate Tag	&lt;ul&gt;&lt;li&gt;Laser-cut acrylic auto tag shows off your favorite team.&lt;/li&gt;&lt;li&gt;Stays in place of license plate where states allow.&lt;/li&gt;&lt;li&gt;Can also hang in back window, decorate rooms, etc.&lt;/li&gt;&lt;li&gt;Team name laser-cut into acrylic and hand-assembled.&lt;/li&gt;&lt;li&gt;Officially licensed by the San Francisco 49ers and the NFL.&lt;/li&gt;&lt;/ul&gt;	\N	\N
42643491	NFL San Francisco 49ers Plush Seat Cover	This Plush Seat Cover features your favorite team's logo embroidered for great quality. It's made of soft, poly-velour material. The seat cover fits bucket seats with adjustable headrests. Straps and skirt hooks keep the seat cover in place for a snug fit.	\N	\N
23129365	Winning Streak San Francisco 49ers Pigskin Pennant	A uniquely hand-crafted, vintage style &quot;pigskin&quot; pennant featuring intricate embroidery and applique design detail.	\N	\N
49584432	Colin Kaepernick San Francisco 49ers Autographed Riddell Mini Helmet - Fanatics Authentic Certified	NULL	\N	\N
21779896	San Francisco Giants Official MLB .5 inch  Earrings by Wincraft	NULL	\N	\N
45983093	MLB San Francisco Giants Carabiner Lanyard Key Ring	Show your team spirit with Carabiner Lanyard Key Ring. Features your teams logo, carabiner clip and key ring.	\N	\N
17118430	MLB San Francisco Giants 20" x 30" Tufted Bath Rug	Durable and attractive; the official MLB Small Tufted Rug by The Northwest Company matches your favorite baseball team's colors. The team's logo is vividly printed on this rug. This Official MLB Rug is perfect to use as an accent rug or utility mat in your home or at the office! No true fan should be without one! This product is made with 100 percent tufted nylon with non-slip rubber backing. Spot clean only.	\N	\N
17754217	San Francisco 49ers Official NFL Laptop Bag by Siskiyou 399039	NULL	\N	\N
45919986	Baby Fanatic NFL Velcro-Closure Bib and High-Top Pre-Walker Set, San Francisco 49ers	Officially licensed gear for your little fan. The Baby Fanatic's Team Bib is a cotton front with embroidered team logo and polester backing to prevent spills on clothes. A velcro closure keeps bib secure. One size fits most. It also includes a set of High-Top style Pre-Walkers. Officially licensed high-top pre-walkers will keep your baby stepping high in team style. The Baby high-tops feature a soft fabric upper with white laces and grippers on the bottom with padded soles. A supported top cap ensures stability and support. The primary team logo is printed along the sides facing out. Finger loop on back allows for easy on and off.	\N	\N
45640791	San Francisco 49ers Official NFL  Baby Bib All Pro Style by Wincraft	NULL	\N	\N
17480851	San Francisco Giants Batbrella Umbrella	This folding, auto-open 48&quot; bat umbrella features an authentic wooden bat handle. There are three clear see-thru panels on front perfect for watching the game. The back panels are solid colors with official team logo. The umbrella has a windproof design with fiberglass ribs, spreaders and shaft.	\N	\N
40391817	San Francisco Giants Glitter Chrome license Plate Frame	Let everyone know who you're rooting for this quality bling chrome license plate frame! Officially licensed and is easy to mount around just about any license plate.	\N	\N
17371341	MLB San Francisco Giants Fanmats Baseball Mat	This Baseball shaped area rug is Chromojet printed for logos with true team colors.	\N	\N
47828221	San Francisco 49ers Official NFL Football Bracelet by Gamewear 022515	&lt;ul&gt;&lt;li&gt;Support your team by wearing a team color football bracelet!&lt;/li&gt;&lt;li&gt;This handmade bracelet is made of genuine quality leather.&lt;/li&gt;&lt;li&gt;The bracelet also features a ceramic football bead and elastic loops for closure.&lt;/li&gt;&lt;li&gt;There are two loops for adjustable sizing, making sure your bracelet is secure.&lt;/li&gt;&lt;li&gt;One size fits all, and will fit most children and adults.&lt;/li&gt;&lt;li&gt;Not recommended for children under 5.&lt;/li&gt;&lt;li&gt;Officially licensed by the San Francisco 49ers and the NFL.&lt;/li&gt;&lt;/ul&gt;	\N	\N
20463191	MLB 3-Piece BBQ Set with Canvas Case, San Francisco Giants	This stainless steel 3-piece BBQ tool set includes a large spatula with built-in bottle opener, heavy-duty tongs and a large fork. All the tools feature a team logo on the handle. The set comes with a durable canvas bag that has a chrome accented team logo. It's perfect for tailgating or any backyard cookout! This BBQ set features all the tools you need for a successful cookout and is easy to transport with the convenient carrying case. You will be the envy of any party with your officially licensed BBQ tool set!	\N	\N
51046988	San Francisco Giants Official MLB Dbl Head Strin by Logo Chair Inc. 068167	NULL	\N	\N
17480866	San Francisco Giants Golf Umbrella	This folding auto-open 62&quot; golf umbrella folds to 17&quot; to fit easily in most purses and backpacks. It features two-tone panels in team colors, with a one-color or full-color imprint on one panel. The umbrella has a windproof design with fiberglass ribs, spreaders and high-quality wood shaft. It also includes an I.D. handle with team logo.	\N	\N
39475293	NFL San Francisco 49ers Netbook/Apple iPad Sleeve	This Netbook/Apple iPad Sleeve is great for holding tablets and e-readers. It feature a high-quality inner core with an embroidered team logo patch on the mesh fabric. Display your team spirit with this stylish sleeve.	\N	\N
40236340	NFL San Francisco 49ers 3' x 5' Flag	This three-foot by five-foot flag is printed in your favorite team's colors. This nylon flag has a reinforced sewn header with 3 grommets for flying your team colors!	\N	\N
21554334	NFL San Francisco 49ers Hip Flask	Show support for your favorite team with Simran International's striking, large capacity, 8 oz.	\N	\N
24904890	San Francisco 49ers Embroidered Leather Tri-Fold Wallet	NULL	\N	\N
21876378	NFL Backpack Cooler by Picnic Time - PTX, San Francisco 49ers - Red	The PTX Backpack Cooler is so versatile and handy you will wonder how you have managed to live your active lifestyle without it! With a fully-insulated interior liner that's also water-resistant you can use it as a food and beverage tote or as an all-around activity tote. With a variety of expandable pockets and front-mounted stretch cord the PTX Backpack Cooler can accommodate and store items beyond its visual confines. It's compact, comfortable and accommodating... the perfect all-around backpack cooler for people on the go!	\N	\N
35151274	MLB 16 oz San Francisco Giants Foldable Water Bottle Set, 2pk	Show off your team spirit with this MLB San Francisco Giants Foldable Water Bottle Set. These reusable containers are a smart, compact way to take water with you on the go. Officially licensed by the NFL, they sport a prominent team logo. These BPA-free 16-oz water bottles each feature a mouthpiece with an attached cap, so you can sip without spills.	\N	\N
19593336	San Francisco 49ers Official NFL License Plate Frame by Siskiyou 385704	NULL	\N	\N
198385917	San Francisco Giants Sandlot Duffel	Screenprinted team logo. Black fabric with distressed logo and retro styling. Dual handles and removable shoulder strap. Dimensions: 22&quot; x 9&quot; x 12&quot;. Cotton material.	\N	\N
17506272	Franklin Sports NFL San Francisco 49ers Deluxe Uniform Set Costume	The Franklin NFL Uniform Set lets young football fans engage in the sport while dressed in the likeness of their favorite team. Sized to fit kids between the ages of four and six years, it includes the pieces they need to play safely and have fun. The San Francisco fournineers Franklin Uniform Set comes with a helmet with the San Francisco fournineers insignia, a mesh team jersey, pants with an elastic waistband for a comfortable fit and a helmet chin strap. There is also a set of iron-on numbers so wearers can emulate their favorite player.	5	3
45701196	NFL San Francisco 49ers Helmet Fan Mask	This mask features an elastic strap for a comfortable, secure fit. Not made for actual game play. Not intended for children under 13 years of age.	\N	\N
19500685	San Francisco Giants Official MLB 16 inch  x 11 inch  Waste Basket by Wincraft, Inc.	NULL	\N	\N
48633313	MLB San Francisco Giants Cactus Dog Tokidoki 6" Plush	A new addition to the tokidoki universe: Perfect for any fans of the Tokidoki series This super plush comes with a special team crown with embroidered Tokidoki logos.	\N	\N
38225442	NFL San Francisco 49ers Street Sign	This Street Sign features your favorite team's logo screen-printed for great quality. Street signs are made of durable styrene and measure four inches by 24 inches.	5	\N
14919971	San Francisco 49ers Official NFL Infant Hooded Infant Towel by McArthur	NULL	\N	\N
21369780	FANMATS NFL - San Francisco 49ers 4x6 Rug	NULL	\N	\N
35907771	NFL Picnic Basket Set by Picnic Time, Champion - San Francisco 49ers, Red	The Champion picnic basket has deluxe service for two, including two wine glasses, two porcelain plates, stainless steel flatware and two napkins that match the basket's interior. Made of willow with dark brown leatherette accents, the Champion has full-wrapping closure straps, an overlapping lid and sturdy suitcase-style leatherette handle. Available with red, navy or black interior lining and matching napkins. Also available with your favorite team's logo on both the napkins and plates.	\N	\N
47828106	San Francisco Giants Baseball Bracelet - Classic Style	&lt;ul&gt;&lt;li&gt;Support your team by wearing a classic baseball seam bracelet!&lt;/li&gt;&lt;li&gt;This handmade bracelet is made of genuine quality leather.&lt;/li&gt;&lt;li&gt;The bracelet also features a ceramic baseball bead and elastic loops for closure.&lt;/li&gt;&lt;li&gt;There are two loops for adjustable sizing, making sure your bracelet is secure.&lt;/li&gt;&lt;li&gt;One size fits all, and will fit most children and adults.&lt;/li&gt;&lt;li&gt;Not recommended for children under 5.&lt;/li&gt;&lt;li&gt;Officially licensed by the San Francisco Giants and the MLB.&lt;/li&gt;&lt;/ul&gt;	\N	\N
48322768	San Francisco 49ers NFL New Era  Mesh Trucker Snapback Hat Red+GT Wristband	NULL	\N	\N
156645695	San Francisco Giants Women's Majestic MLB "Loving The Game" Dual Blend Shirt	NULL	\N	\N
45700887	NFL San Francisco 49ers Foamhead Hat	This 3-dimensional team mascot hat is decorated in your team's colors.	\N	\N
186004280	San Francisco 49ers NFL "Stripes" Men's Boardshorts Swim Trunks	NULL	\N	\N
48322333	San Francisco 49ers New Era Vintage Adjustable hat - Red	NULL	\N	\N
35175320	22oz NFL San Francisco 49ers Slider Top Tumbler with 4 Colored Replacement Straws	Show off your favorite NFL team with this officially licensed slider top tumbler. Use this tumbler at home or take your favorite beverage to go. Rubberized sleeve makes for easy grip, double walled feature keeps your drink from sweating and the slider top makes it easy to open and close. This BPA-free cup is also decorated with your team's colored logo. Even comes with four propeller straws that are compatible with this tumbler.	\N	\N
45699753	NFL San Francisco 49ers iPhone 5/5S Polymer Snap Case	This Polymer Snap Case features access to all ports and buttons and printed full color graphics.	\N	\N
51805771	Official NFL San Francisco 49ers Car Flag 536794	NULL	\N	\N
40382387	Baby Fanatic MLB Velcro-Closure Bib and High-Top Pre-Walker Set, San Francisco Giants	Officially licensed gear for your little fan. The Baby Fanatic's Team Bib is a cotton front with embroidered team logo and polester backing to prevent spills on clothes. A velcro closure keeps bib secure. One size fits most. It also includes a set of High-Top style Pre-Walkers. Officially licensed high-top pre-walkers will keep your baby stepping high in team style. The Baby high-tops feature a soft fabric upper with white laces and grippers on the bottom with padded soles. A supported top cap ensures stability and support. The primary team logo is printed along the sides facing out. Finger loop on back allows for easy on and off.	\N	\N
22108273	FANMATS MLB San Francisco Giants Golf Putting Mat	Take a break and chill out while practicing your short game from the comfort of your own home or office with the FanMats one-foot six-inch x six-foot Indoor Putting Green. Designed for indoor leisure, the mats are officially licensed and fashioned to decorate your home or office in support of your favorite team. The vinyl backing adds longer life and stronger durability, while the synthetic nylon offers an authentic surface to put on compared to an outdoor golf course.	\N	\N
44299546	San Francisco 49ers White Plastic License Plate Frame	Show team pride on the open road with a license plate frame by Rico Industries. Designed for license plates, this heavy-duty frame features pre-drilled holes for mounting. Your team's name and home turf are laser cut on the colorful insert.Officially licensed by the NFL and the San Francisco 49ers.	\N	\N
22281048	San Francisco Giants Grill Mat	NULL	\N	\N
26639376	Wincraft WIN-2901210 San Francisco 49ers NFL Round Wall Clock	NULL	\N	\N
16643351	MLB Baby Fanatic Baby Bottle, 2pk, San Francisco Giants	Get your little one started on the road to fandom with this two-piece baby bottle set from Baby Fanatic. Each one features a silicone nipple, boasts a measuring scale on the side and is boldly decorated with colorful team graphics. They are BPA- and phthalate-free and suitable for newborns.	\N	\N
23085214	C Collectables NFL 12x15 San Francisco 49ers All-Time Greats Plaque	NULL	\N	\N
21266779	NFL San Francisco 49ers Sparkle Lanyard	This officially licensed Sparkle Lanyard features screen print graphics and glitter accents plus a metal utility clip and keyring with detachable buckle.	\N	\N
29372894	NFL San Francisco 49ers 6-Card Plaque, 13x20	All pieces installed in a ready-to-hang plaque. It looks absolutely gorgeous in a home, business or office. Impress your friends, family, employees and colleagues with this hanging on your wall. A must have for all true fans!	\N	\N
46355066	MLB Mens Palladium San Francisco Giants Cufflinks with Gift Box PD-SFG-PP NEW	NULL	\N	\N
197464620	San Francisco 49ers 47 Brand NFL "Munson MVP" Structured Adjustable Hat -  Grey	NULL	\N	\N
189844490	San Francisco Giants MLB Mitchell & Ness 8th Inning Vintage Men's Baseball Shirt	NULL	\N	\N
42620507	NFL San Francisco 49ers Tire Cover	This Tire Cover features your favorite team logo printed on durable vinyl for great quality. Each tire cover fits up to a 16-inch trim rim and can be tightened with a draw string. It's a durable and stylish way to support your team.	\N	\N
46302338	San Francisco MLB Game Time Set	A great way to get started with your OYO collection. Each play set includes two OYO team characters, one umpire, exclusive to this set, two gloves, one bat, a ball and 78 OYO building block field pieces. 84 total pieces in the boxed set make this a great gift for any fan!&nbsp;Please Note: Field Sets do not include actual team players but team themed characters with unique names and jersey numbers. Infield Sets are compatible with other notable building block toys like Lego, K'nex, Mega Bloks and more.	\N	\N
44773711	Pets First MLB San Francisco Giants Pet Tee Shirt	NULL	\N	\N
35117707	NFL San Francisco 49Ers 3-Compartment Lunch Container, 2pk	Show off your allegiance to your favorite football team all year long with this 2-pack of NFL San Francisco 49Ers 3-Compartment Lunch Containers. Officially licensed by the NFL, this handy set displays the popular team's logo on the lids and is offered in the team's unique, vibrant colors. These school lunch containers are BPA-free to ensure your safety and have three different compartments, allowing you to keep your food separated. This functional set is great for traveling or for your next tailgating party.	\N	\N
52497152	San Francisco 49ers Official NFL Kid's Sunglasses by Siskiyou 034251	NULL	\N	\N
136437403	San Francisco 49ers Coffee Mug - 14 Oz Mocha	NULL	\N	\N
179217678	San Francisco Giants Official MLB Fan Bead Bracelet by Siskiyou 306570	NULL	\N	\N
135959493	San Francisco Giants Official MLB Crystal Dangle Earrings by Siskiyou 337604	NULL	\N	\N
40562623	San Francisco Giants MLB 3in x 5in Banner Flag	NULL	\N	\N
165961495	San Francisco Giants 47 Brand MLB "Frost MVP" Structured Adjustable Hat - Black	NULL	\N	\N
49537547	Tokidoki Sticky Note Cube  San Francisco Giants	NULL	\N	\N
40853807	San Francisco Giants Official MLB 1 inch  Lapel Pin by Wincraft	NULL	\N	\N
169568220	San Francisco Giants Official MLB Key Ring by Siskiyou 210600	NULL	\N	\N
199439032	San Francisco 49ers New Era NFL "Logo Topper 2" Performance Bucket Hat	NULL	\N	\N
162123789	San Francisco Giants Majestic MLB "Third Wind" Men's Hooded Sweatshirt	NULL	\N	\N
53977925	Willie McCovey San Francisco Giants Autographed Big Stick Bat with HOF 86 Inscription	NULL	\N	\N
45698223	NFL San Francisco 49ers 3D Football Magnet, 2-Pack	This holographic magnet features nifty team logo graphics that will proclaim your team spirit all along the open road. When you slap this magnet on your car, everyone will see you're a proud fan.	\N	\N
48303835	C Collectibles San Francisco 49ers 2014 Score NFL Football Factory Sealed 13 Card Team Set	NULL	\N	\N
35174726	22oz NFL San Francisco 49ers Straw Tumbler with 4 Colored Replacement Propeller Straws	Show off your favorite NFL team with this officially licensed straw tumbler. Enjoy your favorite beverage cheering on your team or even on the go. Rubberized sleeve makes for easy grip, double walled feature keeps your drink from sweating and propeller straw is ideal for mixing drinks. This BPA-free cup is also decorated with your team's colored logo.	\N	\N
119118657	San Francisco 49ers NFL "Gradient" Men's Boardshorts Swim Trunks	NULL	\N	\N
158831482	San Francisco Giants 47 Brand MLB "Cooler MVP" Structured Adjustable 2 Tone Hat	NULL	\N	\N
104787134	San Francisco 49ers Majestic NFL Zone Blitz V Performance Shirt	NULL	\N	\N
45983281	MLB San Francisco Giants Bottle Opener Key Chain	Be ready to open all your favorite bottle beverages with the Bottle Opener Key Ring. Features full color team logo. Works great on canned beverages too!	\N	\N
48308093	San Francisco 49ers Beach Towel	30 x 60 beach towel. Great for the bathroom, too. Extremely vibrant colors. Machine washable.	\N	\N
42124646	NFL San Francisco 49ers Greats 11" x 14" Deluxe Frame	This frame has one eight- by 10-inch photo that is double-matted.	\N	\N
42519081	San Francisco 49ers Official NFL 3 inch  Leather Key Chain Keychain by Rico Industries	NULL	\N	\N
42812074	San Francisco Giants MLB Manmade Leather Tri-Fold	&lt;ul&gt;&lt;li&gt;The genuine leather tri-fold wallet from Rico Industries holds approximately four credit cards and has a clear plastic slot for your identification.&lt;/li&gt;&lt;li&gt;It's adorned with the full-color team logo embroidered on the front and comes with a plastic picture insert, so you can show off your favorite photos from the game!&lt;/li&gt;&lt;li&gt;Officially licensed by the MLB and the San Francisco Giants.&lt;/li&gt;&lt;/ul&gt;	\N	\N
48603994	San Francisco Giants Official MLB One Size Leather Billfold Wallet by Rico Industries	NULL	\N	\N
53977962	Matt Cain San Francisco Giants Autographed Majestic Replica Beige Jersey	NULL	\N	\N
35174943	22oz MLB San Francisco Giants Straw Tumbler with 4 Colored Replacement Propeller Straws	Show off your favorite MLB team with this officially licensed straw tumbler. Enjoy your favorite beverage cheering on your team or even on the go. Rubberized sleeve makes for easy grip, double walled feature keeps your drink from sweating and propeller straw is ideal for mixing drinks. This BPA-free cup is also decorated with your team's colored logo.	\N	\N
52192107	San Francisco 49ers 'All Time Greats' Plaque	NULL	\N	\N
45698195	NFL San Francisco 49ers 3D Foam D-Fence Sign	This 3D Foam D-Fence Sign is made of double-layered foam and has a hook and loop fastener closure on the back for easy gripping.	\N	\N
188907988	San Francisco 49ers Women's Majestic NFL "Draft Me" Jersey Top Shirt - White	NULL	\N	\N
45983358	MLB San Francisco Giants Camo Carabiner Key Ring	Show your team spirit with Camo Carabiner Lanyard Key Ring. Features your teams logo, carabiner clip and key ring.	\N	\N
48322504	San Francisco 49ers Mitchell & Ness Throwback Logo Snapback Adjustable Hat	NULL	\N	\N
35118615	24oz NFL San Francisco 49Ers Squeeze Water Bottle with Filter Feature	Show off your favorite NFL team with this officially licensed squeeze water bottle. Sure to hydrate you and comes in a vibrant team color with team logo. Easy grip and squeeze technique is ideal for working out, in the car, on the go or anywhere. Comes with a filter feature option that is sold separately.	\N	\N
172337233	San Francisco 49ers Stainless Steel Thermo Can - 16.9 Ounces	NULL	\N	\N
162949481	San Francisco Giants Sunglasses - Dynasty 2. 0 Orange with Black Tips	NULL	\N	\N
113903330	San Francisco Giants MLB Full Comforter Bed in a Bag (Soft & Cozy) (76in x 86in)	NULL	\N	\N
43948788	NFL San Francisco 49ers Super Bowl XLVII Champions Chrome Zippo Lighter Multi-Colored	NULL	\N	\N
40555242	San Francisco 49ers Glitter Chrome Frame	.	\N	\N
199181766	San Francisco 49ers Women's G-III NFL "Complete Game" Scoop Neck Tank Top	NULL	\N	\N
112212044	San Francisco 49ers Women's Majestic NFL "Strong Play" Heathered Sweatpants	NULL	\N	\N
40276132	San Francisco 49ers NFL Laser Cut Black License Plate Frame	NULL	\N	\N
111331249	San Francisco 49ers NFL "Dots" Men's Casual Polyester Walking Shorts	NULL	\N	\N
114974469	San Francisco 49ers New Era 9FIFTY 2016 Draft Gray Snapback Hat - Black Visor	NULL	\N	\N
138589739	Ladies 3/4 Sleeve San Francisco 49ers Tee	NULL	\N	\N
197694652	San Francisco Giants Majestic MLB Tandem Cooperstown V-Neck Men's Fashion Jersey	NULL	\N	\N
193288903	San Francisco Giants Majestic MLB "Heart & Soul" Men's Short Sleeve T-Shirt	NULL	\N	\N
45698126	NFL San Francisco 49ers Bling iPhone 4/4S Applique	This officially licensed applique gives your phone an added layer of protection. Includes full adhesive backing for easy peel-and-stick-application.	\N	\N
23085861	C Collectables NFL 12x15 Montana-Rice San Francisco 49ers Player Plaque	NULL	\N	\N
176059236	San Francisco Giants Official MLB Sling Backpack by Logo Chair Inc. 525899	NULL	\N	\N
198608005	Will Clark San Francisco Giants Mitchell & Ness Authentic 1989 BP Jersey	NULL	\N	\N
53407514	New Era San Francisco Giants MLB 9Fifty Snapback Baseball Cap SIZE: ADJ	NULL	\N	\N
194627275	San Francisco Giants Official MLB Flexi Key Chain by Siskiyou 136535	NULL	\N	\N
42124694	NFL Jerry Rice San Francisco 49ers 11" x 14" Card Frame	This frame has one eight- by 10-inch photo that is double-matted.	\N	\N
40848805	San Francisco Giants Official MLB 18 inch  Necklace by Wincraft	NULL	\N	\N
44724612	San Francisco Giants Official MLB 8 oz. Mini Baseball Helmet Ice Cream Snack Bowls (8) by Rawlings	NULL	\N	\N
21914977	NFL Reflex Travel Couch by Picnic Time, San Francisco 49ers - Black	The Reflex is a portable reclining travel couch with armrests and an adjustable shoulder strap that folds in half for easy transport and storage. With a 37.5-inch long seat the Reflex is roomy enough to seat two adults. High density three-inch thick foam in the seat and back rest make this seat very comfortable. Lay it flat and stretch out for a nap. Four backrest positions offer a range of comfort possibilities. The Reflex provides indoor or outdoor seating possibilities. It's the ultimate chair for tailgating spectator sports indoor TV/movie viewing video gaming and dorm living.	\N	\N
50950627	Jerry Rice San Francisco 49ers Autographed 1986 Topps Rookie #161 Card - Fanatics Authentic Certified	NULL	\N	\N
35138661	MLB 16 oz San Francisco Giants Glass Jar with Lid and Handle, 2pk	Show off your passion for your team with this glass jar with lid and handle.	\N	\N
50770310	Pablo Sandoval Signed 16x20 San Francisco Giants Photo Collage (MLB Auth; SSM 3rd Party Holo)	NULL	\N	\N
162280204	San Francisco 49ers 47 Brand NFL "Game Time" Black Stretch Fit Hat	NULL	\N	\N
42502819	MLB Unisex San Francisco Giants Team Support Lapel Pin PD-SFG-LP NEW	NULL	\N	\N
133003117	San Francisco 49ers NFL G-III "Baseline" Men's Boardshorts Swim Trunks	NULL	\N	\N
45699557	NFL San Francisco 49ers iPhone 4/4S Polymer Snap Case	This Polymer Snap Case features access to all ports and buttons and printed full color graphics.	\N	\N
48293045	San Francisco 49ers NFL Metal Tag License Plate	&lt;b&gt;Features&lt;/b&gt;.	\N	\N
50154363	OYO Sports MLB 84-Piece Infield Set 2.0, San Francisco Giants	This building block play set is a great gift for sports fans young and young at heart. 84 total pieces in this boxed set make this a great gift for any fan! Infield Sets are compatible with other notable building block toys like Lego, K'nex, Mega Bloks and more.	\N	\N
196017419	San Francisco Giants Coffee Mug - 18oz Game Time	NULL	\N	\N
53407686	47 Brand San Francisco Giants MLB Breakaway Knit Beanie Hat SIZE: ONE SIZE FITS MOST	NULL	\N	\N
46302365	OYO Sports NFL Generation 3 Trainer Cart, San Francisco 49ers	Get back in the game with the Trainer Cart from OYO. The officially licensed OYO Trainer Carts are the perfect way to keep your OYO figures in shape for the big game.&nbsp;Official team lettering is printed on the carts. Each Trainer Cart set includes one OYO team trainer character minifigure, one team hat, two footballs, three water bottles, 17 stickers for customization and moving wheels.&nbsp;With 135 pieces in the boxed set, this is a great gift for any fan. Printed and assembled in the USA OYO Trainer Cart sets are compatible with other notable building block toys such as Lego, K'nex, Mega Bloks and more.	5	2
35907821	NFL Picnic Basket by Picnic Time, Champion - San Francisco 49ers, Black	The Champion picnic basket has deluxe service for two, including two wine glasses, two porcelain plates, stainless steel flatware and two napkins that match the basket's interior. Made of willow with dark brown leatherette accents, the Champion has full-wrapping closure straps, an overlapping lid and sturdy suitcase-style leatherette handle. Available with red, navy or black interior lining and matching napkins. Also available with your favorite team's logo on both the napkins and plates.	\N	\N
53407917	47 Brand San Francisco Giants MLB Clean Up Visor SIZE: ADJ	NULL	\N	\N
47714290	MLB San Francisco Giants Dodge Charger (Cruizin' Series 1 25 Scale) 625933	NULL	\N	\N
14669331	MLB - Men's San Francisco Giants Embroidered Billfold Wallet	&lt;p&gt;This MLB San Francisco Giants Embroidered Men's Billfold Wallet makes a stylish accessory for a loyal fan. It is made of genuine cowhide leather and features an embroidered team logo in full color. This leather billfold wallet comes packaged in a metal gift tin.&lt;/p&gt;	\N	\N
35240528	16oz MLB San Francisco Giants Satin Etch Glass Tankard Set, 2pk	Show off your favorite MLB team with this set of officially licensed glass tankards. Uniquely decorated with a satin etch pattern and holds 16-ounces of your favorite beverage. It is decorated with your team's colored logo.	\N	\N
110755193	San Francisco Giants MLB Twin Comforter Set (Soft & Cozy) (64" x 86")	NULL	\N	\N
163386440	San Francisco 49ers Majestic NFL "Critical Victory 2" Men's T-Shirt - Gold	NULL	\N	\N
47827966	San Francisco 49ers NFL Lanyard with Nylon Wallet	&lt;ul&gt;&lt;li&gt;This lanyard with attached nylon ID case is perfect for work or the game!&lt;/li&gt;&lt;li&gt;The ID case has a zipper pocket to put your ID, credit card or money in.&lt;/li&gt;&lt;li&gt;The ID case is printed with your team's logo and outlined with beads.&lt;/li&gt;&lt;li&gt;The lanyard is encrusted with a double row of team color beads.&lt;/li&gt;&lt;li&gt;It is approximately 22&quot; long.&lt;/li&gt;&lt;li&gt;The ID case is approximately 3&quot;x2&quot; in size.&lt;/li&gt;&lt;li&gt;Officially licensed by the San Francisco 49ers and the NFL.&lt;/li&gt;&lt;/ul&gt;	\N	\N
159751584	San Francisco Giants 47 Brand MLB "Woodall" Adjustable Cotton Twill Hat	NULL	\N	\N
46646062	NFL San Francisco 49ers Team Flag, 3' x 5'	This officially licensed three-foot by five-foot flat has attached D-rings. Made of 100 percent polyester, this flag is imprinted with long-lasting color-fast dye. Fly on a flagpole or hang it on a wall.	\N	\N
135112807	Joe Montana San Francisco 49ers Majestic Men's HOF Eligible Receiver III T-Shirt	NULL	\N	\N
48044169	Mizco NFL San Francisco 49ers Armor Stereo Hands-F	NULL	\N	\N
121843559	San Francisco Giants Majestic MLB "Series Sweep" Cooperstown Men's S/S T-Shirt	NULL	\N	\N
136048061	San Francisco 49ers NFL 47 Brand "Gait" 3 Pack Colorblocked Men's No Show Socks	NULL	\N	\N
53407804	47 Brand San Francisco Giants MLB Sparkle Strapback Baseball Cap SIZE: ADJ	NULL	\N	\N
44626009	NFL San Francisco 49ers Pendant Bracelet, 8"	Show off your team spirit in this NFL chain bracelet. A colorful team logo charm dangles from a polished link chain, which secures with a toggle clasp to complete the design.	\N	\N
35757317	Picnic Time Urban Basket, Black San Francisco 49ers Digital Print	The Urban Basket is a folding collapsible canvas basket that's as versatile as it is stylish. It has a stationary form-fitted aluminum handle, to which the accordion-style canvas body folds for compact storage and a snap closure keeps the basket folded. The padded interior base flattens out to keep the basket erected or folds in half lengthwise for storage. Use the Urban Basket for shopping or to transport items at or away from home.	\N	\N
115163208	Will Clark San Francisco Giants Majestic Cooperstown Cool Base White Jersey	NULL	\N	\N
156521461	San Francisco 49ers Youth NFL "Accelerate" Short Sleeve T-Shirt	NULL	\N	\N
21841359	NFL Cooler Tote by Picnic Time - Topanga, San Francisco 49ers - Red	The Topanga cooler tote is the perfect all-around bag. It's made of durable polyester with complementing stitching accents to enhance its simple yet sophisticated lines. The interior is made of heat-sealed PVC for no leaks and it's large enough to carry 24 soda cans. All materials exceed Federal and state product safety standards, so you can be assured the Topanga is safe. It's perfect for carrying food and drinks to the beach or pool and features a separate exterior pocket that's the perfect size for your wallet or keys. It can also be used to carry your wet swimsuits and towels on the trip home. Just pack it up and go with the Topanga!	\N	\N
21030473	NFL San Francisco 49ers 20-Ounce Travel Mug	This 20-Ounce Travel Mug features a built in bottle opener under the handle. It is decorated with a high quality metal team logo.	\N	\N
109298952	San Francisco Giants Majestic MLB "Team Choice" On-Field Hooded Sweatshirt	NULL	\N	\N
148291348	San Francisco 49ers Women's Majestic NFL Gameday Gal VI Long Sleeve Shirt	NULL	\N	\N
44775661	Pets First MLB San Francisco Giants Pet Jersey	NULL	\N	\N
161061148	Maxx HD Sunglasses San Francisco Giants Dynasty 2 Black w/Orange Stitch V-Touch	NULL	\N	\N
153340575	Jerry Rice San Francisco 49ers Women's Majestic NFL "HOF Fair Catch V" T-Shirt	NULL	\N	\N
122669018	San Francisco 49ers Majestic NFL "Gridiron" Men's Premium Reflective T-Shirt	NULL	\N	\N
52496413	San Francisco 49ers Official NFL Chrome Wrap Sunglasses by Siskiyou 292736	NULL	\N	\N
197331981	San Francisco 49ers New Era 9Forty NFL "The League Graph Pop" Adjustable Hat	NULL	\N	\N
158578755	San Francisco 49ers Women's G-III NFL "Hail Mary" Tri-blend 3/4 Sleeve T-shirt	NULL	\N	\N
132615354	San Francisco 49ers 47 Brand NFL "Wright" Clean Up Adjustable Hat - Khaki	NULL	\N	\N
33024501	Hunter Mfg DN-303752 San Francisco 49ers Deluxe Dog Jersey - Medium	Pet jerseys are constructed of a polyester material, offering a v-neck cut and sizing to allow for a comfort fit (high cut on stomach area for the male dog). Jerseys are decorated in full team colors with official team logos. This is machine washable.	\N	\N
159114762	San Francisco 49ers Youth NFL "Flex" Structured Adjustable Hat	NULL	\N	\N
53978179	Madison Bumgarner San Francisco Giants Autographed 2014 World Series Baseball with 14 WS MVP Inscription	NULL	\N	\N
53977978	Willie McCovey San Francisco Giants Autographed Baseball with 1959 NL ROY Inscription	NULL	\N	\N
116012697	San Francisco 49ers Mitchell & Ness NFL "Sweep" Full Zip Hooded Sweatshirt	NULL	\N	\N
53407752	American Needle San Francisco Giants MLB Raglan Strapback Baseball Cap SIZE: ADJ	NULL	\N	\N
52696778	Rawlings RAW-07171084121 San Francisco 49ers NFL Youth Size Team Color Hail Mary Football	NULL	\N	\N
47710130	Kindle Fire Vintage Baseball Cover - San Francisco	NULL	\N	\N
15689353	San Francisco Giants MLB Laser Cut License Plate Tag	NULL	\N	\N
119491659	San Francisco 49ers New Era NFL 39THIRTY Gray & Black Neo Flex Fit Hat	NULL	\N	\N
122232674	San Francisco 49ers New Era NFL Super Bowl Champions "Big Team" Knit Hat	NULL	\N	\N
52496826	San Francisco 49ers Official NFL Tall Leather Wallet by Siskiyou 341557	NULL	\N	\N
40375436	San Francisco 49ers Official NFL Artisan Pilsner Glass by Boelter Brands 254811	''Experience the unparalleled look and quality of hand painted products where technique and designs stand second to none! Created with non-toxic materials and exceptionally made with pride, using exquisite detail and pure craftsmanship, these pieces are truly one-of-a-kind and will be cherished by all recipients.	\N	\N
107720970	San Francisco 49ers Majestic NFL Kick Return 2 Men's Charcoal Hooded Sweatshirt	NULL	\N	\N
183395645	San Francisco 49ers New Era NFL 39THIRTY Team Color Stitched Neo Flex Fit Hat	NULL	\N	\N
40276006	San Francisco 49ers Official NFL 12 inch x 6 inch  Chrome License Plate Frame by Rico Industries	&lt;ul&gt;  &lt;li&gt;Show everyone who you root for with this chrome license plate frame!&lt;/li&gt;  &lt;li&gt;Features your favorite team's name and logo and has pre-drilled holes for easy mounting.&lt;/li&gt;  &lt;li&gt;The chrome frame is very durable and will last for a long time!&lt;/li&gt;  &lt;li&gt;Makes for a great gift!&lt;/li&gt;  &lt;li&gt;Officially licensed by the NFL and the San Francisco 49ers.&lt;/li&gt;&lt;/ul&gt;	\N	\N
107276695	Ladies San Francisco 49ers Long Sleeve Henley Tee	NULL	\N	\N
181514033	San Francisco 49ers NFL G-III "Play Action" Reversible F/Z Hooded Sweatshirt	NULL	\N	\N
114567265	San Francisco 49ers Majestic NFL "Strong Will" Men's Synthetic Shorts	NULL	\N	\N
50244622	San Francisco 49ers Plastic Pint Glass Set	NULL	\N	\N
42519851	San Francisco 49ers Official NFL 3.5 inch  Small Static Cling Window Car Decal by Rico Industries	&lt;ul&gt;&lt;li&gt;Officially licensed re-usable static cling logo decal.&lt;/li&gt;&lt;li&gt;Static cling logo decal measure 3.5 x 3.75 inches.&lt;/li&gt;&lt;li&gt;Officially licensed by the NFL and the San Francisco 49ers.&lt;/li&gt;&lt;/ul&gt;	\N	\N
155330481	San Francisco Giants Official MLB Victory Backpk by Logo Chair Inc. 067672	Screenprinted team logo. Multiple pockets. Exterior mesh pocket for water bottle. Inner compartment for 13&quot; laptop. Rich black fabric with team color accents. Dimensions: 10&quot; x 6&quot; x 16&quot;. Polyester material.	\N	\N
125964852	San Francisco 49ers Women's Majestic NFL "Franchise Fit" Short Sleeve T-shirt	NULL	\N	\N
180291786	San Francisco 49ers Women's G-III NFL "Game Time" V-neck Slub T-shirt	NULL	\N	\N
179863893	San Francisco 49ers Women's G-III NFL "Opening Day" Jersey Tank Top	NULL	\N	\N
129266302	Tim Lincecum San Francisco Giants Youth Majestic MLB Player Black T-Shirt	NULL	\N	\N
45641024	San Francisco Giants Official MLB  Lapel Pin by Wincraft	NULL	\N	\N
52496778	San Francisco 49ers Official NFL Toothbrush by Siskiyou 280757	NULL	\N	\N
45983390	MLB San Francisco Giants Plush Team Fuzzy Dice	Hang them in your car, man cave, office or kids room, the possibilities are endless. These MLB Fuzzy Dice are made of high quality plush and each are three inches in size. The two dice come on a string to easily hang anywhere. Cruise in true style when you spruce up your ride with these spiffy plush team fuzzy dice!	\N	\N
16642179	San Francisco Giants MLB Laser Chrome Frame	&lt;ul&gt;&lt;li&gt;Laser chrome license plate frame.&lt;/li&gt;&lt;li&gt;Acrylic team inserts at top and bottom of frame Decorated in team colors.&lt;/li&gt;&lt;li&gt;Officially licensed by the San Francisco Giants and the MLB.&lt;/li&gt;&lt;/ul&gt;	\N	\N
198107505	San Francisco 49ers New Era NFL "Visor Lined" Performance Visor	NULL	\N	\N
43348007	Legends Never Die San Francisco 49ers Greats Framed Memorabili	NULL	\N	\N
19264582	San Francisco 49ers NFL Super Bowl Commemorative Woven Tapestry Throw (48"x60")	NULL	\N	\N
23085764	C Collectables NFL 12x15 Joe Montana San Francisco 49ers Career Stat Plaque	NULL	\N	\N
50906620	Framed Willie Mays Autographed San Francisco Giants Grey Jersey - MAYS HOLO	NULL	\N	\N
28617075	Victory Collectibles VIC-000089-30333 Jerry Rice Autographed Throwback San Francisco 49ers Replica Helmet	NULL	\N	\N
130450675	Womens San Francisco 49ers Long Sleeve Jersey Tee	NULL	\N	\N
163613668	Joe Montana Signed San Francisco 49ers Logo Football JSA	NULL	\N	\N
47929545	San Francisco 49ers Official NFL Barbell Tongue Ring by Siskiyou 436734	NULL	\N	\N
37405217	NFL San Francisco 49ers Then and Now 12x18 Double Frame	Two licensed photos, double matted and installed in a ready to hang 12x18 frame.	\N	\N
24906548	San Francisco 49ers Garden Gnome - 11'' Male	NULL	5	1
52652810	MLB San Francisco Giants Iphone 4/4S SF Large Logo Lenticular Case 049117	NULL	\N	\N
136002976	San Francisco 49ers Majestic NFL "Right Direction" Men's S/S Premium T-Shirt	NULL	\N	\N
46360248	MLB San Francisco Giants Van Metro Water Bottle	This polyethylene 22-ounce water bottle with a push pull cap is surrounded by a three-millimeter neoprene rubber sleeve to reduce condensation and help keep the contents at an even temperature longer. It features a sewn-in bottom and your favorite team logo and colors.	\N	\N
19416803	San Francisco 49ers Mini Pennants - 8 Piece Set	NULL	\N	\N
48309637	San Francisco Giants Beach Towel by McArthur	NULL	\N	\N
180380393	San Francisco 49ers New Era 2015 NFL Sideline Sport Knit Hat- Yellow/Graphite	NULL	\N	\N
160747273	San Francisco 49ers New Era NFL Prep Class Cuffed Knit Hat	NULL	\N	\N
21129702	Reynolds Wrap Aluminum Foil, 150 sf	This Reynolds Wrap Aluminum Foil can be used for many purposes around the home. Use it to wrap leftover food or to protect food from freezer burn. Reynolds Wrap Foil can also be used to cover bowls and help keep food moist while also preventing splatters. With this 150 SF Reynolds Aluminum Foil, you can make cleanup easy by lining pans before cooking. It is strong enough for your tough food needs.	4.55700016	65
103134196	San Francisco Giants Majestic MLB "Dominant" V-Neck Men's Gray Fashion Jersey	NULL	\N	\N
43250211	SF Giants Embossed Metal Sign	Features: -MLB collection. -Material: Heavy duty steel. -Realistic weathered design and embossed lettering. -Riveted in all four corners for easy hanging. -Perfect addition for fan caves, offices, bedroom or recreation rooms. Subject: -Sports and sports teams. Gender: -Men's/Boys. Size: -Mini 17'' and under. Style: -Vintage. Color: -Multi-colored. Time Period: -Contemporary. Product Type: -Memorabilia. Region: -North America. Primary Art Material: -Metal. Dimensions: Overall Height - Top to Bottom: -14.5''. Overall Width - Side to Side: -11.5''. Overall Depth - Front to Back: -0.25''. Overall Product Weight: -2 lbs.	\N	\N
187611100	San Francisco Giants Majestic MLB "Official Logo" Cooperstown Men's S/S T-Shirt	NULL	\N	\N
40372298	Pro Specialties Group PSG-Z242944 San Francisco 49ers NFL Oven Mitt and Pot Holder Set	NULL	\N	\N
145452575	San Francisco 49ers Men's NFL Polyester Jogger Pants	NULL	\N	\N
50904187	Colin Kaepernick San Francisco 49ers Autographed 2011 Topps Draft Silver #220 Card - Fanatics Authentic Certified	NULL	\N	\N
46646383	NFL San Francisco 49ers Team Flag, 3' x 5', Style 2	This officially licensed three-foot by five-foot flat has attached D-rings. Made of 100 percent polyester, this flag is imprinted with long-lasting color-fast dye. Fly on a flagpole or hang it on a wall.	\N	\N
42519593	San Francisco 49ers Official NFL 1 inch x 1 inch  Fingernail Tattoo Set by Rico Industries	&lt;ul&gt;&lt;li&gt;You get four sheets of five fingernail-sized tattoos. Enough to cover TWENTY fingernails&lt;/li&gt;&lt;li&gt;Sizes vary for differing size nails.&lt;/li&gt;&lt;li&gt;Easy to apply.&lt;/li&gt;&lt;li&gt;Takes only about 30 seconds.&lt;/li&gt;&lt;li&gt;Easy to remove using rubbing alcohol or baby oil.&lt;/li&gt;&lt;li&gt;Vibrant colors and crisp graphics.&lt;/li&gt;&lt;li&gt;Official team logos and colors.&lt;/li&gt;&lt;li&gt;Officially licensed by the league and the team.&lt;/li&gt;&lt;li&gt;A terrific gift idea.&lt;/li&gt;&lt;li&gt;Officially licensed by the NFL and the San Francisco 49ers.&lt;/li&gt;&lt;/ul&gt;	\N	\N
23085407	C Collectables MLB 12x15 Tim Lincecum San Francisco Giants 8-Card Plaque	NULL	\N	\N
185734569	Barry Bonds San Francisco Giants Signed Rawlings Baseball Bat JSA+Steiner	NULL	\N	\N
102641256	San Francisco Giants Black License Plate Frame	NULL	\N	\N
35757294	Picnic Time San Francisco 49ers Tahoe Cooler	The Tahoe wasn't designed solely as a beach bag, but if a beach bag is what you're looking for this one won't disappoint! This extra large tote has almost one cubic feet of interior storage space, enough to hold 48 12-ounce cans! Fully-insulated to keep your food and drinks cold, the Tahoe also has a heat-sealed water-resistant interior liner which is perfect for transporting wet pool towels swim suits or the like. A larger zipper pocket on the exterior of the tote lets you keep other personal effects within easy reach. The Tahoe may be just the family-sized beach style tote you've been wishing you had.	\N	\N
22029932	San Francisco 49ers Official NFL Blade Sunglasses by Siskiyou 165597	NULL	\N	\N
170903470	San Francisco 49ers New Era NFL 39THIRTY Team Front Neo Flex Fit Hat	NULL	\N	\N
22225807	Sports Rug - San Francisco Giants  (4 ft. x 6 ft.)	NULL	\N	\N
50630357	Colin Kaepernick & Jim Harbaugh San Francisco 49ers Dual Autographed Riddell Pro Line Authentic Helmet - Fanatics Authentic Certified	NULL	\N	\N
38467907	NFL 16 oz San Francisco 49Ers Colin Kaepernick Players Jersey Glass Pint, Set of 2	Show off your favorite team and your favorite player with this officially licensed glass pint set! Beautifully decorated with licensed team-colored logo and jersey with your players name, number and signature on it! Holds 16 ounces of your favorite beverage and is dishwasher safe for easy cleaning.	\N	\N
38769489	San Francisco 49ers Wood Sign - 11"x17" Fan Cave Design	NULL	\N	\N
109887487	San Francisco 49ers Mitchell & Ness NFL "Oatmeal" Cuffed Knit Hat w/ Pom	NULL	\N	\N
52495081	San Francisco 49ers Official NFL Classic Chain Necklace by Siskiyou 130755	NULL	\N	\N
46075174	San Francisco 49ers Official NFL 1 inch  Fence Lapel Pin by Wincraft	NULL	\N	\N
22948711	Sports Coverage Inc. MLB San Francisco Giants Rod Pocket Drape Panel	NULL	\N	\N
40381865	Duckhouse 16 Ounce Mason Jar - San Francisco 49ers	NULL	\N	\N
143231283	San Francisco 49ers Majestic NFL "Line of Scrimmage 2" Men's T-Shirt - Red	NULL	\N	\N
51496082	Framed Jerry Rice San Francisco 49ers Autographed 8" x 10" Red Running Solo Photograph - Fanatics Authentic Certified	NULL	\N	\N
107855508	San Francisco 49ers NFL Medalist Men's T-shirt & Flannel Pajama Pants Sleep Set	NULL	\N	\N
23129491	Winning Streak San Francisco 49ers Classic Pennant	A uniquely hand-crafted, vintage style wool pennant featuring intricate embroidery and applique design detail.	\N	\N
162742207	San Francisco 49ers New Era 9FIFTY 2015 Gold Collection Sideline Snap Back Hat	NULL	\N	\N
52496396	San Francisco 49ers Official NFL Small Family Decal Set by Siskiyou 304651	NULL	\N	\N
194716046	San Francisco Giants Buster Posey Baseball Bracelet - Team C	NULL	\N	\N
101233515	San Francisco 49ers Women's Majestic NFL "Believe" V-neck Heathered T-shirt	NULL	\N	\N
110321582	San Francisco 49ers New Era NFL Energy Pipe Performance Visor	NULL	\N	\N
43348037	Legends Never Die San Francisco 49ers Big 3 All-Time Framed Memorabili	NULL	\N	\N
52496814	San Francisco 49ers Official NFL Zippered Sunglass Case by Siskiyou 148811	NULL	\N	\N
183347488	San Francisco 49ers New Era 39THIRTY NFL 2014 On-Field Performance Flex Hat	NULL	\N	\N
52265063	Colin Kaepernick San Francisco 49ers Framed Autographed 20'' x 24'' In Focus Throwing Photograph - Fanatics Authentic Certified	NULL	\N	\N
47829113	San Francisco 49ers Tumbler - Square Insulated (16oz)	NULL	\N	\N
45700897	NFL San Francisco 49ers Football	This genuine Wilson leather fits most beverage cans and bottles. It features authentic pigskin with laces and the inner foam lining keeps your beverage cold while you enjoy the game.	\N	\N
100260848	Colin Kaepernick San Francisco 49ers Youth NFL Mid Tier Replica Jersey	NULL	\N	\N
42519281	San Francisco 49ers Die Cut Pennant	NULL	\N	\N
51805809	San Francisco 49ers Official NFL Oval Chrome Key Chain by Siskiyou 387272	NULL	\N	\N
50623173	Colin Kaepernick San Francisco 49ers Autographed Red Jersey with NFL QB Record 181 Yds Rushing Inscription - Fanatics Authentic Certified	NULL	\N	\N
189556593	San Francisco 49ers New Era NFL 2015 Training Sideline Performance Visor - White	NULL	\N	\N
194204259	San Francisco Giants MLB Full Comforter Set (Soft & Cozy) (76" x 86")	NULL	\N	\N
35291639	Sparo MLB Men's San Francisco Giants Icon Watch	The one watch men will want more than anything else. The genuine leather double contrast stitched padded strap has a cushion feel on the wrist. The full color logo on the sub second dial makes it a unique watch. Talk about iconic. The steel dial with points of fluorescent detailing and silver fluorescent hands shines and makes this watch stand out on a class of its own. The Icon watch features a high-precision quartz movement, stainless steel caseback and a scratch-resistant mineral crystal lens. It is water and shock resistant to 3 ATM (99ft) and comes with a limited lifetime warranty.	\N	\N
24519186	MLB San Francisco Giants Seat Cover	Make your ride the pride of the team with this high-quality seat cover from Fremont Die. This soft polyester velour seat cover features a team color embroidered logo and fits all bucket seats. It is held in place with adjustable straps and skirt hook.	\N	\N
46944613	Forever Collectibles MLB 2015 Santa Hat, San Francisco Giants	Enjoy the holidays with your favorite team! Show your team spirit with this officially licensed Santa Hat. Each Santa hat has your favorite team's logo and colors; it's fun for all ages.	\N	\N
43347736	Legends Never Die San Francisco 49ers Joe Montana Collage Framed Memorabili	NULL	\N	\N
45698123	NFL San Francisco 49ers Bling Galaxy S4 Applique	This officially licensed applique gives your phone an added layer of protection. Includes full adhesive backing for easy peel-and-stick-application.	\N	\N
41804608	San Francisco Giants Fan Favorite Recliner	NULL	\N	\N
128442346	San Francisco 49ers NFL G-III "Center Field" Men's Boardshorts Swim Trunks	NULL	\N	\N
50904002	Framed Willie McCovey San Francisco Giants Autographed Cream Jersey with HOF 1986 Inscription	NULL	\N	\N
115180966	Buster Posey San Francisco Giants MLB Majestic Player Black T-Shirt	NULL	\N	\N
48875884	San Francisco Giants Winning Streak Past Mascots Wool Heritage Banner (8"x32")	NULL	\N	\N
45699577	NFL San Francisco 49ers Game Day Face Temporary Tattoo	This Game Day Face Temporary Tattoo has an easy water-based application but won't sweat off, with easy peel-away removal. As Seen on Shark Tank!	\N	\N
100756148	San Francisco 49ers Majestic NFL Line of Scrimmage VII Men's T-Shirt - Charcoal	NULL	\N	\N
50887801	Jerry Rice, Joe Montana & Steve Young San Francisco 49ers Autographed Throw Back Proline Helmet with Multiple Inscriptions - Fanatics Authentic Certified	NULL	\N	\N
33076518	San Francisco Giants Official MLB 2 inch  Metal Spinner Key Chain Keychain by Rico Industries	This Officially Licensed Spinner Keychain features a metal key ring with a spinning piece to show off your team's graphics and logo. This keychain is perfect for keys and badges and is a cool way to show off your team spirit!	\N	\N
52645614	San Francisco Giants Bam Vino Bottle Holder	NULL	\N	\N
42518964	San Francisco 49ers Official NFL 2 inch  Crystal View Key Chain Keychain by Rico Industries	&lt;ul&gt;&lt;li&gt;Use this colorful Crystal View Keychain to carry your team pride with you all year long.&lt;/li&gt;&lt;li&gt;Officially licensed by the NFL and the San Francisco 49ers.&lt;/li&gt;&lt;/ul&gt;	\N	\N
107938491	San Francisco 49ers Majestic "Action" Men's F/Z Therma Base Hooded Sweatshirt	NULL	\N	\N
118628450	San Francisco Giants New Era MLB Logo Whiz Cuffed Knit Hat with Pom	NULL	\N	\N
47829136	NFL San Francisco 49ers Coaster (Set Of 4)	&lt;ul&gt;&lt;li&gt;Perfect for protecting your furniture, these heavy-duty vinyl coasters are sold in sets of four.&lt;/li&gt;&lt;li&gt;Beautifully decorated with team logos and colors, these coasters feature a non-slip surface perfect for setting down your drinks.&lt;/li&gt;&lt;li&gt;Officially licensed by the San Francisco 49ers and the NFL.&lt;/li&gt;&lt;/ul&gt;	\N	\N
52497221	San Francisco 49ers Official NFL Ribbon Magnet by Siskiyou 107603	NULL	\N	\N
46349504	MLB San Francisco Giants 16-Ounce Freezer Mug	Have an icy cold drink with your favorite team without the ice! This 16-Ounce Freezer Mug is double-walled and filled with re-freezable crystals. Simply store the mug in your freezer for an hour and enjoy a chilled drink without a watered down effect. This mug is officially licensed with wraparound high definition team graphics to not only make your drinks taste better but look better too!	5	1
100011830	San Francisco 49ers Majestic NFL "Field Goal" Men's Crew Sweatshirt	NULL	\N	\N
41154750	San Francisco Giants Official MLB 4 inch x 4 inch  Perfect Cut Car Decal by Wincraft	NULL	\N	\N
155528380	San Francisco 49ers 47 Brand NFL "Slot Back" Clean Up Adjustable Hat	NULL	\N	\N
156924587	San Francisco 49ers New Era NFL Super Bowl XXIII Champions Bucket Hat	NULL	\N	\N
142205157	Willie Mays San Francisco Giants Signed 1966 Official Baseball Guide JSA	NULL	\N	\N
183773921	San Francisco 49ers Women's G-III NFL "Catch" Cotton Slub V-neck T-shirt	NULL	\N	\N
132902367	San Francisco 49ers Youth NFL "Frequency" Long Sleeve T-Shirt	NULL	\N	\N
50624246	Colin Kaepernick San Francisco 49ers Autographed 8'' x 10'' Roll Out Pass Photograph - Fanatics Authentic Certified	NULL	\N	\N
52660556	San Francisco 49ers Official NFL Kwikset Key by Siskiyou 822667	NULL	\N	\N
50622284	Framed Colin Kaepernick San Francisco 49ers Autographed Red Jersey with NFL QB Record 181 Yds Rushing Inscription - Fanatics Authentic Certified	NULL	\N	\N
161606245	San Francisco 49ers NFL "Oxford" Men's Woven Silk Tie	NULL	\N	\N
39474729	EuroGraphics San Francisco Golden Gate Bridge 1000-Piece Puzzle	&lt;br&gt;&lt;b&gt;EuroGraphics San Francisco Golden Gate Bridge 1000-Piece Puzzle:&lt;/b&gt;&lt;ul&gt;&lt;li&gt;Strong high-quality puzzle pieces&lt;/li&gt;&lt;li&gt;Made from recycled board and printed with vegetable-based ink&lt;/li&gt;&lt;li&gt;Theme: city collection&lt;/li&gt;&lt;li&gt;1000-piece puzzle&lt;/li&gt;&lt;li&gt;Box size: 10&quot;W x 14&quot;D x 2.37&quot;H&lt;/li&gt;&lt;li&gt;Finished size: 19.25&quot; x 26.5&quot;&lt;/li&gt;&lt;/ul&gt;	\N	\N
109590996	San Francisco 49ers 47 Brand NFL "Draft Day" Closer Mesh Back Stretch Hat	NULL	\N	\N
130708701	San Francisco 49ers NFL "Acclaim" Men's Cotton Flannel Boxer	NULL	\N	\N
49500044	Topps 2013 Team Set - San Francisco Giants	NULL	\N	\N
101066641	San Francisco 49ers 47 Brand NFL "McNally" Throwback Mesh Back Adjustable Hat	NULL	\N	\N
51805853	San Francisco Giants Official MLB License Plate Frame by Siskiyou 269257	NULL	\N	\N
21089170	NFL Backpack Pal - San Francisco 49Ers	&lt;p&gt;Let everyone know which team you support with this officially licensed NFL Backpack Pal. This bag featuring the team mascot and colors for the San Francisco 49ers is made of 100 percent polyester material. The plush backpack can be worn using the drawstring straps to the big game or while out running errands. This cute and cuddly backpack makes an ideal gift for any 49ers fan.&lt;/p&gt;	\N	\N
45640563	San Francisco 49ers Official NFL  Lapel Pin by Wincraft	NULL	\N	\N
112419962	San Francisco Giants 14oz. Full Wrap Travel Mug	NULL	\N	\N
50905784	Steve Young San Francisco 49ers Autographed February 18, 1993 Sports Illustrated - Fanatics Authentic Certified	NULL	\N	\N
40168354	NFL Player 10" Plush Doll San Francisco 49ers, Patrick Willis	Bring your favorite NFL player home! This 10-inch plush figure is the perfect gift for young fans and collectors. Bleacher Creatures takes your favorite player and transforms him into a lovable character to play with, encouraging fun, inspiration and play.	\N	\N
165097683	San Francisco 49ers Youth NFL "Helmet" Full Zip Sweatshirt	NULL	\N	\N
50625128	Joe Montana San Francisco 49ers Autographed Red Pro-Line Jersey - Fanatics Authentic Certified	NULL	\N	\N
155483579	San Francisco 49ers NFL 47 Brand Vintage Gray "Tri-State Retro" Premium T-Shirt	NULL	\N	\N
120765556	San Francisco 49ers Majestic NFL "Fanfare 8" Men's Short Sleeve Cool Base Shirt	NULL	\N	\N
183693690	San Francisco 49ers Majestic NFL "Great Move" Men's 3/4 Sleeve Tri-Blend T-Shirt	NULL	\N	\N
183485647	San Francisco 49ers 47 Brand NFL "Game Time" Red Stretch Fit Hat	NULL	\N	\N
139818762	San Francisco Giants New Era MLB "Winter Fresh" Cuffed Knit Hat with Pom	NULL	\N	\N
165616249	San Francisco 49ers Majestic NFL "Red Zone" Men's Short Sleeve Premium T-Shirt	NULL	\N	\N
52496628	San Francisco 49ers Chrome Keychain	NULL	\N	\N
50903153	Jerry Rice San Francisco 49ers Autographed Wilson Pro Football with HOF 2010 Inscription - Fanatics Authentic Certified	NULL	\N	\N
187436725	San Francisco 49ers Youth NFL "Stated" Full Zip Hooded Sweatshirt	NULL	\N	\N
176359523	San Francisco Giants Official MLB Chain Necklace & Team Pendant Siskiyou 273469	NULL	\N	\N
194338956	San Francisco 49ers NFL "Facade" Men's Micro Fleece Pajama Pants	NULL	\N	\N
53977867	Rawlings Brian Wilson San Francisco Giants Autographed Baseball	NULL	\N	\N
126101940	San Francisco 49ers New Era 9FIFTY NFL 2015 Draft Snapback Hat - Gray	NULL	\N	\N
52265270	Madison Bumgarner San Francisco Giants Autographed Baseball and Mahogany Baseball Display Case	NULL	\N	\N
109388520	San Francisco Giants Mardi Gras Beads with Medallion	NULL	\N	\N
127466135	San Francisco Giants New Era 9Forty MLB "The League Shadow" Adjustable Hat	NULL	\N	\N
47855611	Hunter 2014 World Series Champions San Francisco G	NULL	\N	\N
45698101	NFL San Francisco 49ers Big Spatula/Bottle Opener	This officially licensed spatula and bottle opener has a wooden handle and your team's logo laser-etched into the stainless steel spatula blade.	\N	\N
42519707	San Francisco 49ers Official NFL 1 inch  Lapel Pin by Wincraft	NULL	\N	\N
187050373	San Francisco 49ers NFL "Gradient" Men's Polyester Training Shorts	NULL	\N	\N
123556405	San Francisco 49ers New Era NFL Super Bowl Champion Logo's Bucket Hat	NULL	\N	\N
116376778	San Francisco Giants Baseball Bracelet - Team Color Style, Orange by Gamewear	NULL	\N	\N
163761615	San Francisco 49ers Majestic NFL 2015 Reflective Short Sleeve Men's Red T-Shirt	NULL	\N	\N
186616181	San Francisco 49ers Women's Majestic NFL "Draft Me 2" Jersey Top Shirt	NULL	\N	\N
42507916	NFL Mens Silver-Tone Metal San Francisco 49ers Football Sports Suit Lapel Pin	NULL	\N	\N
42519016	San Francisco Giants Official MLB 18 inch  Necklace by Wincraft	NULL	\N	\N
142179646	San Francisco 49ers New Era 9Forty NFL "The League Shadow" Adjustable Hat	NULL	\N	\N
181651360	San Francisco Giants Coffee Mug - 14oz Sculpted, Orange by Boelter Brands	NULL	\N	\N
21676529	San Francisco Giants Official MLB 7 inch  Charm Bracelet by Wincraft	NULL	\N	\N
16934596	San Francisco 49ers 24"x36" Wool Dynasty Banner	&lt;ul class=&quot;noindent&quot;&gt;&lt;li&gt;Hand-crafted banner&lt;/li&gt;&lt;li&gt;Made of wool&lt;/li&gt;&lt;/ul&gt;	5	2
50624971	Madison Bumgarner San Francisco Giants Autographed Black Game Used Cleats with 14 Game Used and WS MVP Inscription	NULL	\N	\N
118377065	San Francisco Giants Majestic MLB "Electric Ball" Short Sleeve Men's T-Shirt	NULL	\N	\N
156087164	San Francisco 49ers New Era NFL 2015 Training Camp Sideline Bucket Hat - White	NULL	\N	\N
51483139	Jerry Rice San Francisco 49ers Autographed 8" x 10" Red Running Solo Photograph - Fanatics Authentic Certified	NULL	\N	\N
44758913	Legends Never Die San Francisco 49ers Candlestick Park Framed Memorabili	NULL	\N	\N
41154768	San Francisco Giants Official MLB 5.5 inch x 5 inch  Car Window Cling Decal by Rico Industries	NULL	\N	\N
50622533	Framed Joe Montana San Francisco 49ers Autographed Red Proline Jersey with HOF 2000 Inscription - Fanatics Authentic Certified	NULL	\N	\N
22246082	MLB San Francisco Giants Classic Frozen Rope Baseball Necklace	Show off your favorite Major League Baseball team by wearing this officially licensed Frozen Rope Necklace from GameWear. Each necklace is made from genuine twisted baseball leather to create the look of a frozen rope line drive. Necklaces feature three colorful beads, adorned with your favorite team's logo and colors. Necklaces are one-size-fits-all and have a unique elastic baseball bead closure for a loose and comfy fit.	\N	\N
20745904	San Francisco 49ers NFL Lanyard w/Key Ring Pro Specialties Group 250602	NULL	\N	\N
165308108	San Francisco Giants Official MLB Hard Shell Glasses Case by Siskiyou 312687	NULL	\N	\N
17338982	San Francisco Giants MLB Mini Pennant Set (8)	&lt;ul&gt;&lt;li&gt;The best part about going to college is getting to support your school team!&lt;/li&gt;&lt;li&gt;Show off your team spirit in the dorms or at home with this Mini Pennant set.&lt;/li&gt;&lt;li&gt;This set comes with 8 pennants with official team logos.&lt;/li&gt;&lt;li&gt;Officially licensed by the MLB and the San Francisco Giants.&lt;/li&gt;&lt;/ul&gt;	\N	\N
167905566	San Francisco 49ers Mitchell & Ness NFL Premium Long Sleeve Rugby Shirt	NULL	\N	\N
48306578	San Francisco 49Ers Jersey Banner 34" x 30" - 2-Sided  - NFL Licensed #93998B	NULL	\N	\N
52495840	San Francisco 49ers Official NFL I Heart Game Day Shades by Siskiyou 462955	NULL	\N	\N
39958794	San Francisco Giants Official MLB Kid's Sunglasses by Siskiyou 244179	Our designer women's sunglasses have a repeating logo design on the team colored arms and rhinestone accents on the frames around the lenses.	\N	\N
141530103	San Francisco 49ers 47 Brand NFL "Palmer" Cuffed Knit Hat	NULL	\N	\N
42519132	San Francisco Giants Die Cut Pennant	&lt;ul&gt;&lt;li&gt;Nothing says you're a die hard fan more than this felt die cut pennant in the teams colors.&lt;/li&gt;&lt;li&gt;The shaped pennant is the perfect finishing touch to any loyal fans bar or party room.&lt;/li&gt;&lt;li&gt;Officially licensed by the MLB and the San Francisco Giants.&lt;/li&gt;&lt;/ul&gt;	\N	\N
37405511	NFL San Francisco 49ers 12x18 Double Frame	Two licensed photos, double matted and installed in a ready to hang 12x18 frame.	\N	\N
\.


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

