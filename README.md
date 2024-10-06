# Thread-safe ZeroMQ
This implementation is based on Golang's [zmqchan](https://github.com/abligh/zmqchan), all the credit should go the the author of [zmqchan](https://github.com/abligh/zmqchan)

This is just a tweaked implementation in Rust

```
                        +-------------------+
                        |     Client        |
                        +-------------------+
                                    |
                                    v
                            (Client Sends Message)
                                    |
+-----------------------------------v----------------------------------------+
|                          ChannelPair                                       |
|                                                                            |
|                           +--------------+                                 |
|                           |    z_sock    |                                 |
|                           | (ZeroMQ SOCK)|                                 |
|                           +--------------+                                 |
|                                   |                                        |
|                                   v                                        |
|                     (Message Forwarded to Channel)                         |
|                                   |                                        |
|                          +--------------+                                  |
|                          |   channel    |                                  |
|                          | (Crossbeam   |                                  |
|                          |   Channel)   |                                  |
|                          +--------------+                                  |
|                                   |                                        |
|                                   v                                        |
|                    (Message Consumed by `z_tx[IN]`)                        |
|                           +---------------+                                |
|                           |   z_tx[IN]    |                                |     
|                           | (PAIR Socket) |                                | 
|                           +---------------+                                |
|                                   |                                        |
|                                   |                                        |
|                       (Message Sent to `z_tx[OUT]`)                        |                        
|                                   |                                        |
|                                   v                                        | 
|                           +---------------+                                |
|                           |   z_tx[OUT]   |                                |
|                           | (PAIR Socket) |                                |
|                           +---------------+                                |
|                    (Message Passed to `z_sock`)                            |
|                                   |                                        |
|                                   |                                        |
|                    (Message Prepared for `z_sock`)                         |
|                                   v                                        |
|                           +--------------+                                 |
|                           |   z_sock     |                                 |
|                           | (Writable)   |                                 |
|                           +--------------+                                 |
|                                   |                                        |
|                    (Message Sent Back to Client)                           |
|                                   |                                        |
+----------------------------------------------------------------------------+
                                    |
                                    v
                        (Client Receives Response)
                                    |
                            +-------------------+
                            |      Client       |
                            +-------------------+
```