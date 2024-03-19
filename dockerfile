# Use the official Node.js image from Docker Hub
FROM --platform=linux/amd64 node:16.18

# Set the working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json to the working directory
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the application code to the working directory
COPY . .

# Expose the port that your application will run on
EXPOSE 9285

# Define the default command to run your application
CMD ["npm", "start"]
