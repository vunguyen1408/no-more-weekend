


#Output one image every second, named out1.png, out2.png, out3.png, etc.

ffmpeg -i input.flv -vf fps=1 out%d.png

#Output one image every minute, named img001.jpg, img002.jpg, img003.jpg, etc. The %03d dictates that the ordinal number of each output image will be formatted using 3 digits.

ffmpeg -i myvideo.avi -vf fps=1/60 img%03d.jpg

#Output one image every ten minutes:

ffmpeg -i test.flv -vf fps=1/600 thumb%04d.bmp



#-vframes option

#Output a single frame from the video into an image file:

ffmpeg -i input.flv -ss 00:00:14.435 -vframes 1 out.png

#This example will seek to the position of 0h:0m:14sec:435msec and output one frame (-vframes 1) from that position into a PNG file.

#select video filter

#Output one image for every I-frame:

ffmpeg -i input.flv -vf "select='eq(pict_type,PICT_TYPE_I)'" -vsync vfr thumb%04d.png

