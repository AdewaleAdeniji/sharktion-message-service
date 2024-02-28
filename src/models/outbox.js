import mongoose from "mongoose";
const Schema = mongoose.Schema;

const outboxSchema = new Schema(
  {
    rate: { type: String, required: true },
    rateFrom: { type: String, required: true },
    rateTo: { type: String, required: true },
    rateAddedBy: { type: String, required: true },
    rateID: { type: String, required: true, unique: true },
    date: { type: String, required: true, default: getDate() },
    subscriberCalled: { type: Boolean, default: false },
    rateChange: { type: String, default: "0" },
  },
  {
    timestamps: true,
  }
);
exports.rates = rateSchema;
export default mongoose.model("rates", rateSchema);