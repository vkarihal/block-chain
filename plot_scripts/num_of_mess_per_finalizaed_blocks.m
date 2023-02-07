% DELAY = 0.5
%{
stream_total_message = [55158, 150535, 322672, 582971, 958548, 1461219, 2092792, 2902260, 3896900];
pala_total_message = [57265, 217297, 510832, 1238983, 2116094, 3364929, 5747921, 8275426, 11463775];
pipelet_total_message = [31831, 32315, 34999, 50840, 54165, 56992, 71767, 75735, 78327];

stream_total_finalize_blocks = [2812, 2860, 2916, 2987, 2996, 2998, 2999, 2999, 2999];
pala_total_finalize_blocks = [2812, 4162, 4646, 6390, 6642, 6919, 8256, 8564, 8836];
pipelet_total_finalize_blocks = [3976, 2935, 2496, 2986, 2703, 2473, 2755, 2606, 2441];
%}

% DELAY = 1
%{
stream_total_message = [46318, 120966, 255534, 492526, 820843, 1311917, 1995051, 2820775, 3821100];
pala_total_message = [39556, 114045, 263228, 622134, 1058162, 1686566, 2878739, 4124693, 5703821];
pipelet_total_message = [15895, 16183, 18141, 25675, 26975, 28029, 36121, 37942, 39203];

stream_total_finalize_blocks = [2076, 1941, 1915, 2428, 2421, 2590, 2833, 2898, 2932];
pala_total_finalize_blocks = [1983, 2182, 2396, 3213, 3327, 3465, 4138, 4269, 4395];
pipelet_total_finalize_blocks = [1984, 1468, 1292, 1506, 1345, 1214, 1384, 1302, 1220];
%}

%DELAY = 1.5
%{
stream_total_message = [42075, 106044, 222730, 404236, 671971, 1044451, 1572010, 2262727, 3058210];
pala_total_message = [26983, 74300, 173988, 412008, 709134, 1108735, 1906941, 2753006, 3825483];
pipelet_total_message = [10267, 11048, 11971, 17171, 18033, 18691, 24527, 24743, 26363];

stream_total_finalize_blocks = [1583, 1398, 1373, 1799, 1798, 1793, 2101, 2160, 2181];
pala_total_finalize_blocks = [1353, 1420, 1577, 2123, 2224, 2281, 2737, 2846, 2948];
pipelet_total_finalize_blocks = [1281, 1000, 852, 1006, 897, 808, 938, 848, 818];
%}

%DELAY = 2

stream_total_message = [39851, 97300, 201743, 360889, 603144, 943261, 1356736, 1912380, 2606464];
pala_total_message = [21644, 58805, 130552, 312009, 527187, 843563, 1445287, 2068926, 2848825];
pipelet_total_message = [8389, 8093, 8793, 12734, 13567, 14300, 18243, 18703, 19489];

stream_total_finalize_blocks = [1300, 1052, 1002, 1465, 1426, 1463, 1662, 1650, 1693];
pala_total_finalize_blocks = [1097, 1126, 1183, 1607, 1656, 1733, 2075, 2139, 2198];
pipelet_total_finalize_blocks = [1045, 733, 625, 744, 673, 617, 696, 639, 603];


streamlet_message_per_finalized_block = [-1,-1,-1,-1,-1,-1,-1,-1,-1];
pala_message_per_finalized_block = [-1,-1,-1,-1,-1,-1,-1,-1,-1];
pipelet_message_per_finalized_block = [-1,-1,-1,-1,-1,-1,-1,-1,-1];

for k = 1: (length(stream_total_message))
    y = stream_total_finalize_blocks(k);
    z = stream_total_message(k);
    c = z/y;
    streamlet_message_per_finalized_block(k) = c
end

for k = 1: (length(pala_total_message))
    y = pala_total_finalize_blocks(k);
    z = pala_total_message(k);
    c = z/y;
    pala_message_per_finalized_block(k) = c
end

for k = 1: (length(pipelet_total_message))
    y = pipelet_total_finalize_blocks(k);
    z = pipelet_total_message(k);
    c = z/y;
    pipelet_message_per_finalized_block(k) = c
end

semilogy(3:11, streamlet_message_per_finalized_block,'LineWidth',2)
hold on
semilogy(3:11, pala_message_per_finalized_block,'LineWidth',2)
hold on
semilogy(3:11, pipelet_message_per_finalized_block,'LineWidth',2)
hold off

ylabel('Messages per finalized blocks');
xlabel('Number of nodes');
legend('Streamlet','Pala', 'Pipelet');