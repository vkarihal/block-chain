stream_total_finalize_blocks = [2812; 2860; 2916; 2987; 2996; 2998; 2999; 2999; 2999; 2999; 2999; 2999; 2999];
pala_total_finalize_blocks = [2812; 4162; 4646; 6390; 6642; 6919; 8256; 8564; 8836; 10080; 10309; 10674; 11779];
pipelet_total_finalize_blocks = [3976; 2935; 2496; 2986; 2703; 2473; 2755; 2606; 2441; 2666; 2540; 2449; 2617];

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

bar_vals= [stream_total_finalize_blocks, pala_total_finalize_blocks, pipelet_total_finalize_blocks];

bar(bar_vals, 0.5)
name={'n3','n4','n5','n6','n7','n8','n9','n10','n11','n12','n13','n14','n15'};
set(gca,'xticklabel',name);
ylabel('Time per finalized block');
xlabel('Nodes');
legend('Streamlet','Pala', 'Pipelet');