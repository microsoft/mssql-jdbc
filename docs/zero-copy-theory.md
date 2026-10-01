
Search

Get app
Write
Sign up

Sign in


Unknown user

Zhenyuan (Zane) Zhang
Zhenyuan (Zane) Zhang
Software Engineer | Java | Python | JS/TS | BU MSCS

Follow writer

Software Development

Operating Systems

Java

Zero-copy: Principle and Implementation
Zhenyuan (Zane) Zhang
Zhenyuan (Zane) Zhang

Follow
8 min read
·
Jan 10, 2024
131


1

1



1. Introduction
I/O or Input / Output typically means read and write between central processing unit (CPU) and external devices such as disks, mice, keyboards and so on. Before we dive deep into Zero-copy, it is necessary to point out the difference between disk I/O (including disk devices and other block-oriented devices) and network I/O. According to our text book, common interfaces for disk I/O are read(), write() and seek(). Meanwhile, the interface for network I/O, typically, is the socket related interface. What happens under the hood with socket interfaces is that the sender and the receiver computers create their own sockets, and set the connection where they send or receive files. Nowadays, more and more web applications had achieved the switch from CPU bounded to I/O bounded, which means the performance of I/O is typically the bottleneck of those applications.

Other topics that are got involved are Interrupt, Mode Switching and Direct Memory Access (DMA) which I will talk about in detail in next part. In general, users cannot directly operate any data at kernel including read and write. Data must be copied from kernel to user memory, and this operation must be down by CPU, thus introduced lots of performance loss. That’s where Zero-copy come into play. The main principle of Zero-copy is to eliminate or reduce as much as possible the CPU data copy between user memory and kernel memory, and the corresponding interrupts and the number of mode switching are reduced as well, so as to increase the I/O performance within network I/O.

2. Pitfalls of DMA
Direct Memory Access or DMA is a great idea and works perfect when it comes to relief the load of CPU and save it from direct data copy from and to disks. But there are still spaces to improve.

Press enter or click to view image in full size

Figure 1. Sequence diagram of read() with DMA
The typical read() process is shown in Figure 1. First, as application invokes the read() commend, and there is a mode switching from user to kernel. Then the CPU initiates a DMA transfer and the DMA initiates the I/O transfer from disk. To finish this DMA transfer, data should be transmitted to disk cache so that DMA is able to transfer data to kernel buffer. Then DMA interrupts CPU from other stuff to signal transmission completion. Then it is CPU who transfer data form kernel buffer to user buffer. Finally, the other mode switching that from kernel to user happens and return data to application.

Now, let’s take a look at a more complicated example, reading data from disk and write it to internet interface. This operation might be one of the most common operations in web application in client-server mode.

Press enter or click to view image in full size

Figure 2. Read from disk to network interface
In this example, first, a read() command is executed and causes a CPU mode switching, and then triggers the DMA data copy from disk to kernel buffer. Then CPU is responsible for copying data to user buffer with another mode switching from kernel to user. The write() command does something similar with network interface and socket buffer. CPU copies data from user buffer to socket buffer and generate headers and tailers info for transferring, and a mode switching follows. Then DMA copies data to network interface. Finally, a mode switching back to user mode.

We definitely can’t say this process is efficient because a total of four data copies and four times of mode switching really increase the load of system and slow down the response efficiency.

3. Principle of Zero-copy
We have talk about the limitation of DMA. With DMA’s help, there are still two CPU copies along with four times of mode switching even if there is no need for CPU to compute anything. The objective of Zero-copy is simple, we want to eliminate or reduce the unnecessary data copy by CPU between kernel buffer and user buffer, so as to reduce the mode switching as well, and the performance improvement could be then realized.

Zero-copy is a general idea and the common name of a set of implementations. Throughout years, people have been exploring and improving its implementations. For this project, I am going to dive deep into some popular implantations.

4. Implementations of Zero-copy
4.1. Using mmap()
In operating systems, virtual memory (VM) is mapped to physical memory by paging tables. Multiple VM addresses can be mapped to a single physical memory address. The idea of this implementation is mapping the address of user’s virtual memory to the address of kernel memory. So that the CPU don’t have to copy the data back and forth. mmap() stands for memory mapping. It is a system call that can map data in kernel buffer to user memory. What happens behind the scene is the virtual addresses in kernel memory and in user memory are pointing to the same place of physical address (shared-memory).

Press enter or click to view image in full size

Figure 3. Read from disk to network interface with mmap()
As Figure 3 shows, application calls mmap() instead of read(). Since user memory and kernel memory are sharing the same physical memory address, the data in kernel buffer will be directly copied to socket buffer by CPU. No data are copied to user memory in this case.

Get Zhenyuan (Zane) Zhang’s stories in your inbox
Join Medium for free to get updates from this writer.

Enter your email
Subscribe

Remember me for faster sign in

With mmap(), we successfully get rid of one CPU copy in this example. But we still have four times of mode switching, three data copy and the expensive mapping operation on VM. There are spaces to improve further.

4.2. Using sendfiles()
Linux kernel 2.1 provides us a new system call, sendfile(), to replace read() and write() for some use cases like the above example. With one system call instead of two, we are able to eliminate two times of mode switching.

Press enter or click to view image in full size

Figure 4. Read from disk to network interface with sendfile()
Figure 4 shows the process of the same example but using sendfile() to achieve Zero-copy. Application calls sendfile() system call this time. DMA copies data to kernel buffer, and then data are copied to socket buffer by CPU. Compared to the first implementation, using sendfile() brings us two times of mode switching, three data copies including one CPU copy.

4.3. Using sendfiles() with DMA Gather
There are some improvements with sendfile() system call in Linux 2.4, the most important one is the emergence of DMA Scatter/Gather. With this improvement, we finally can eliminate all of the CPU copy in the above scenario and achieve the real Zero-copy.

Press enter or click to view image in full size

Figure 5. Read from disk to network interface with sendfile() with DMA gather
Figure 5 shows the process. As application call sendfile(), DMA controller copies data from disk to kernel buffer by DMA scatter, meaning you don’t need a continuous memory space for data storage. Then CPU appends the file descriptor to the socket buffer and DMA controller generates corresponding header and tailer of the network packet. Lastly, DMA controller follows the description on socket buffer, copies data and then makes packets from kernel buffer to network interface for network transmission.

5. Experiment of Zero-copy using Java
Now we pretty much understand a lot about Zero-copy, the objectives, the principle and multiple implementation methods. For this part, I evaluated the improvement of the performance by using Zero-copy. In Java, there are APIs in the class FileChannel that are using the mechanism of mmap() and sendfile() respectively. For this experiment, I used three methods to copy an 880 MB file on Ubuntu 22.04. The results are as follows.

private static void sendfileCopyFile(String inputFilePath, String outputFilePath) {

    long start = System.currentTimeMillis();

    try (
            FileChannel channelIn = new FileInputStream(inputFilePath).getChannel();
            FileChannel channelOut  = new FileOutputStream(outputFilePath).getChannel();
    ) {
        channelIn.transferTo(0, channelIn.size(), channelOut);

    } catch (IOException e) {
        e.printStackTrace();
    }

    long end = System.currentTimeMillis();
    System.out.println("Total time spent: " + (end - start));
}

private static void mmapCopyFile(String inputFilePath, String outputFilePath) {

    long start = System.currentTimeMillis();

    try (
            FileChannel channelIn = new FileInputStream(inputFilePath).getChannel();
            FileChannel channelOut = new RandomAccessFile(outputFilePath, "rw").getChannel();

    ) {
        long size = channelIn.size();
        MappedByteBuffer mbbi = channelIn.map(FileChannel.MapMode.READ_ONLY, 0, size);
        MappedByteBuffer mbbo = channelOut.map(FileChannel.MapMode.READ_WRITE, 0, size);
        for (int i = 0; i < size; i++) {
            byte b = mbbi.get(i);
            mbbo.put(i, b);
        }

    } catch (Exception e) {
        e.printStackTrace();
    }

    long end = System.currentTimeMillis();
    System.out.println("Total time spent: " + (end - start));
}

private static void bufferInputStreamCopyFile(String inputFilePath, String outputFilePath) {

    long start = System.currentTimeMillis();
    try(
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(inputFilePath));
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFilePath));
    ){
        byte[] buf = new byte[1];
        int len;
        while ((len = bis.read(buf)) != -1) {
            bos.write(buf);
        }

    }catch(Exception e){
        e.printStackTrace();
    }
    long end = System.currentTimeMillis();
    System.out.println("Total time spent: " + (end - start));
}

Figure 6. Results of performance eval using Java 11
6. Conclusion
For this project, I introduced pretty much every fundamental element of Zero-copy. From its background, to design principle and then I introduced three most popular implementations in industry in detail with schematic diagrams. In section of experiment, I use java to simulate the process of data transition. From the result, there are significant improvements with both types of Zero-copy implementations. There is 81% speed improvement with mmap(), and 91% improvement with sendfile(), which answered the question why people are intensively using this technique in the industry. Meanwhile, there are also spaces to improve, such as the high expense of mapping operation, the limited use cases of sendfile() and the space restrictions. Hope we can see more improvements regarding them in the future.

Video Presentation
https://www.youtube.com/watch?v=SLkRYqj6d4E&t=260s

References
[1] washuu, E.B. Zero-copy: Techniques, Benefits and Pitfalls [J].

[2] L. Tianhua, Z. Hongfeng, C. Guira, and Z. Chuansheng. Research and Implementation of Zero-Copy Technology in Linux [C]. 2006 IEEE Sarnoff Symposium, Princeton, NJ, USA, 2006, pp. 1–4. DOI: 10.1109/SARNOF.2006.4534808.

[3] Avi Silberschatz, Peter Baer Galvin, Greg Gagne. Operating System Concepts [M]. Tenth Edition. John Wiley & Sons, Inc., 2018. ISBN 978–1–118–06333–0.

[4] Shawn Xu. It’s All About Buffers: Zero-Copy, MMAP, and Java NIO [EB/OL]. Available at: https://shawn-xu.medium.com/its-all-about-buffers-zero-copy-mmap-and-java-nio-50f2a1bfc05c.

[5] Trung Huynh. Java NIO: Using Memory-Mapped File to Load Big Data into Applications [EB/OL]. Available at: https://medium.com/@trunghuynh/java-nio-using-memory-mapped-file-to-load-big-data-into-applications-5058b395cc9d.

Software Development

Operating Systems

Java

131


1

1


Zhenyuan (Zane) Zhang
Written by Zhenyuan (Zane) Zhang
18 followers
·
35 following
Software Engineer | Java | Python | JS/TS | BU MSCS


Follow
Help

Status

About

Careers

Press

Blog

Store

Privacy

Rules

Terms

Text to speech