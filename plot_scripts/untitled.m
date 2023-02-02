stream_total_message = [55158; 150535; 322672; 582971; 958548; 1461219; 2092792; 2902260; 3896900; 5064397; 6472488; 8117665; 9986316 ];
pala_total_message = [57265; 217297; 510832; 1238983; 2116094; 3364929; 5747921; 8275426; 11463775; 17002354; 22229340; 28839554; 39186284];
pipelet_total_message = [31831; 32315; 34999; 50840; 54165; 56992; 71767; 75735; 78327; 93529; 96805; 100659; 115471];

for k = 1: length(stream_total_message)
    y = (k+2)*3000;
    z = stream_total_message(k);
    c = z/y;
    stream_total_message(k) = c;
end

for k = 1: length(pala_total_message)
    y = (k+2)*3000;
    z = pala_total_message(k);
    c = z/y;
    pala_total_message(k) = c;
end

for k = 1: length(pipelet_total_message)
    y = (k+2)*3000;
    z = pipelet_total_message(k);
    c = z/y;
    pipelet_total_message(k) = c;
end

bar_vals= [stream_total_message, pala_total_message, pipelet_total_message];

bar(bar_vals, 0.3)
name={'n3','n4','n5','n6','n7','n8','n9','n10','n11','n12','n13','n14','n15'};
set(gca,'xticklabel',name);
ylabel('Messages per unit time');
xlabel('Nodes');
legend('Streamlet','Pala', 'Pipelet');