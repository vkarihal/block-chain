% DELAY = 0.5
%{
stream_total_finalize_blocks = [2812, 2860, 2916, 2987, 2996, 2998, 2999, 2999, 2999];
pala_total_finalize_blocks = [2812, 4162, 4646, 6390, 6642, 6919, 8256, 8564, 8836];
pipelet_total_finalize_blocks = [3976, 2935, 2496, 2986, 2703, 2473, 2755, 2606, 2441];
%}

% DELAY = 1
%{
stream_total_finalize_blocks = [2076, 1941, 1915, 2428, 2421, 2590, 2833, 2898, 2932];
pala_total_finalize_blocks = [1983, 2182, 2396, 3213, 3327, 3465, 4138, 4269, 4395];
pipelet_total_finalize_blocks = [1984, 1468, 1292, 1506, 1345, 1214, 1384, 1302, 1220];
%}

% DELAY = 1.5
%{
stream_total_finalize_blocks = [1583, 1398, 1373, 1799, 1798, 1793, 2101, 2160, 2181];
pala_total_finalize_blocks = [1353, 1420, 1577, 2123, 2224, 2281, 2737, 2846, 2948];
pipelet_total_finalize_blocks = [1281, 1000, 852, 1006, 897, 808, 938, 848, 818];
%}

% DELAY = 2
stream_total_finalize_blocks = [1300, 1052, 1002, 1465, 1426, 1463, 1662, 1650, 1693];
pala_total_finalize_blocks = [1097, 1126, 1183, 1607, 1656, 1733, 2075, 2139, 2198];
pipelet_total_finalize_blocks = [1045, 733, 625, 744, 673, 617, 696, 639, 603];

for k = 1: length(stream_total_finalize_blocks)
    y = 3000;
    z = stream_total_finalize_blocks(k);
    c = y/z;
    stream_total_finalize_blocks(k) = c;
end

for k = 1: length(pala_total_finalize_blocks)
    y = 3000;
    z = pala_total_finalize_blocks(k);
    c = y/z;
    pala_total_finalize_blocks(k) = c;
end

for k = 1: length(pipelet_total_finalize_blocks)
    y = 3000;
    z = pipelet_total_finalize_blocks(k);
    c = y/z;
    pipelet_total_finalize_blocks(k) = c;
end

%bar_vals= [stream_total_finalize_blocks, pala_total_finalize_blocks, pipelet_total_finalize_blocks];

%bar(bar_vals, 0.3)
plot(3:11, stream_total_finalize_blocks,'LineWidth',2)
hold on
plot(3:11, pala_total_finalize_blocks,'LineWidth',2)
hold on
plot(3:11, pipelet_total_finalize_blocks,'LineWidth',2)
hold off

%name={'n3','n4','n5','n6','n7','n8','n9','n10','n11','n12','n13','n14','n15'};
%set(gca,'xticklabel',name);
ylabel('Time per finalized block');
xlabel('Number of nodes');
legend('Streamlet','Pala', 'Pipelet');