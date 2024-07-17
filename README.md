# okazii
This repository contains the implementation of a microservices-based auction system. The system is designed using multiple microservices that communicate with each other to handle auction bids, process messages, and determine the auction winner.


# Microservices Overview
AuctioneerMicroservice:
- Listens on port 1500.
- Receives bids from BidderMicroservice instances.
- Forwards received bids to the MessageProcessorMicroservice.
- Receives the auction result from the BiddingProcessorMicroservice and notifies the bidders.

MessageProcessorMicroservice:
- Listens on port 1600.
- Receives bids from the AuctioneerMicroservice.
- Processes and sorts the bids.
- Forwards processed bids to the BiddingProcessorMicroservice.

BiddingProcessorMicroservice:
- Listens on port 1700.
- Receives processed bids from the MessageProcessorMicroservice.
- Determines the auction winner.
- Sends the auction result to the AuctioneerMicroservice.

BidderMicroservice:
- Sends bids to the AuctioneerMicroservice on port 1500.


# Running the Microservices
To run the entire system, you need to start each microservice in a specific order to ensure proper communication.
MessageProcessorMicroservice -> BiddingProcessorMicroservice -> AuctioneerMicroservice -> BidderMicroservice

Start each service:
cd path\to\BidderMicroserviceApp  
dotnet run
