This project is a distributed system designed to analyze simulated financial data in real-time and generate actionable trading signals.The system uses a microservices architecture 
combined with stream processing to ensure that data flows seamlessly from ingestion to analysis. Real-time communication is established via WebSockets, ensuring that users receive 
immediate updates on market trends.

At the core of the system is a data generator script that simulates financial data from various sources. 
This data is validated by an ingestion service and then processed to compute essential trading indicators such as Moving Average, Exponential Moving Average, and Relative Strength Index.
These indicators are used to generate buy/sell signals, which are promptly communicated to users through a notification service. The system also aggregates and visualizes the data in real-time
on a dashboard, offering a clear picture of stock performance.

Designed to be scalable and robust, the architecture allows for efficient load balancing and seamless communication between services. The project is developed collaboratively, and its codebase 
is well-documented to support further enhancements. Additional bonus features include interactive visualization, integration with real-world datasets, and distributed caching solutions to enhance performance.
