stream_total_messages = [55158; 150535; 322672; 582971; 958548; 1461219; 2092792; 2902260; 3896900; 5064397; 6472488; 8117665; 9986316 ];

for k = 1: length(stream_total_messages)
    y = (k+2)*3000
    z = stream_total_messages(k)
    c = z/y
    stream_total_messages(k) = c;
    %disp(stream_total_messages(k))
end