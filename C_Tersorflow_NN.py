from __future__ import print_function

from C_Fund_Analysis import fund_Analysis, fund_data_proprocessing
import numpy as np
import pandas as pd
import tensorflow as tf

beg_date = '2015-01-01'
funds = ['002001_Nav']
df_filtered = fund_Analysis(beg_date, funds)
train_sets, cv_sets, test_sets = fund_data_proprocessing(beg_date, funds, df_filtered)


def getFeatures(samples):
    array_z = np.zeros((1, 395), dtype=np.float32)
    for sample in samples:
        row, col = sample.shape
        columns = sample.columns
        em_rows = 5 - row
        if em_rows > 0:
            df = pd.DataFrame(np.zeros((em_rows, col)), columns=columns)
            sample = pd.concat([sample, df])
        if em_rows < 0:
            sample = sample.iloc[1:, :]

        if array_z[0, 0] == 0:
            array = np.array(sample.values)
            array_z = np.reshape(array, (1, -1))
        else:
            array = np.array(sample.values)
            array_z = np.vstack((array_z, np.reshape(array, (1, -1))))

    return array_z


def getLabels(labels):
    labels = np.array(labels)
    return labels


def getTFDataSets(each_set):
    samples = each_set['sample_sets']
    labels = each_set['label_sets']

    features = getFeatures(samples)
    labels = getLabels(labels)

    tf_features = tf.constant(features, dtype=features.dtype, shape=features.shape)
    tf_labels = tf.constant(labels, dtype=features.dtype, shape=features.shape)
    # return tf.data.Dataset.from_tensor_slices((features, labels))
    return features, labels


train_f, train_l = getTFDataSets(train_sets)
cv_f, cv_l = getTFDataSets(cv_sets)
test_f, test_l = getTFDataSets(test_sets)


# Parameters
learning_rate = 0.00001
num_steps = 50000
batch_size = 128
display_step = 100

# Network Parameters
n_hidden_1 = 1024  # 1st layer number of neurons
n_hidden_2 = 512  # 2nd layer number of neurons
n_hidden_3 = 128  # 2nd layer number of neurons
num_input = 395  # MNIST data input (img shape: 28*28)
num_classes = 3  # MNIST total classes (0-9 digits)

# tf Graph input
X = tf.placeholder("float", [None, num_input])
Y = tf.placeholder("float", [None, num_classes])

# Store layers weight & bias
weights = {
    'h1': tf.Variable(tf.random_normal([num_input, n_hidden_1])),
    'h2': tf.Variable(tf.random_normal([n_hidden_1, n_hidden_2])),
    'h3': tf.Variable(tf.random_normal([n_hidden_2, n_hidden_3])),
    'out': tf.Variable(tf.random_normal([n_hidden_3, num_classes]))
}
biases = {
    'b1': tf.Variable(tf.random_normal([n_hidden_1])),
    'b2': tf.Variable(tf.random_normal([n_hidden_2])),
    'b3': tf.Variable(tf.random_normal([n_hidden_3])),
    'out': tf.Variable(tf.random_normal([num_classes]))
}


# Create model
def neural_net(x):
    # Hidden fully connected layer with 256 neurons
    layer_1 = tf.add(tf.matmul(x, weights['h1']), biases['b1'])
    # Hidden fully connected layer with 256 neurons
    layer_2 = tf.add(tf.matmul(layer_1, weights['h2']), biases['b2'])
    # Hidden fully connected layer with 256 neurons
    layer_3 = tf.add(tf.matmul(layer_2, weights['h3']), biases['b3'])
    # Output fully connected layer with a neuron for each class
    out_layer = tf.matmul(layer_3, weights['out']) + biases['out']
    return out_layer


# Construct model
logits = neural_net(X)
prediction = tf.nn.softmax(logits)

# Define loss and optimizer
loss_op = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(
    logits=logits, labels=Y))
optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate)
train_op = optimizer.minimize(loss_op)

# Evaluate model
correct_pred = tf.equal(tf.argmax(prediction, 1), tf.argmax(Y, 1))
accuracy = tf.reduce_mean(tf.cast(correct_pred, tf.float32))

# Initialize the variables (i.e. assign their default value)
init = tf.global_variables_initializer()

# Start training
with tf.Session() as sess:
    # Run the initializer
    sess.run(init)

    for step in range(1, num_steps + 1):
        # batch_x, batch_y = mnist.train.next_batch(batch_size)
        # Run optimization op (backprop)
        sess.run(train_op, feed_dict={X: train_f, Y: train_l})
        if step % display_step == 0 or step == 1:
            # Calculate batch loss and accuracy
            loss, acc = sess.run([loss_op, accuracy], feed_dict={X: train_f,
                                                                 Y: train_l})
            print("Step " + str(step) + ", Minibatch Loss= " + \
                  "{:.4f}".format(loss) + ", Training Accuracy= " + \
                  "{:.3f}".format(acc))

    print("Optimization Finished!")

    # Calculate accuracy for MNIST test images
    print("Testing Accuracy:", \
          sess.run(accuracy, feed_dict={X: cv_f,
                                        Y: cv_l}))
