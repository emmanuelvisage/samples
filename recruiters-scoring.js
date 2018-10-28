'use strict';
const co = require('co');
const moment = require('moment');
const Promise = require('bluebird');

const Common = require('@visage/visage-server-common');
const Preference = Common.Domain.Preference();

const CandidateSubmission = Common.Domain.CandidateSubmission;

let now;

const WINDOW_WEEKS = 1;
const GROWTH_FACTOR = 10;
const DIMINISHING_FACTOR = 2;

/***
 * Get all submissions by couple (active recruiter, job) during the WINDOW_WEEKS time period
 * @param global {boolean} When true submissions are only grouped by job (not grouped by recruiters)
 * @returns {Promise} A promise that resolve with the result of the mongodb aggregation
 */
const retrieveRecruitersSubmissions = (global) => {
  const from = moment(now).subtract(WINDOW_WEEKS, 'weeks').toDate();
  const to = moment(now).toDate();
  const aggregate = [
    {
      //not accurate but allow to filter a little bit beforehand
      $match: {
        'history.status': 'ExpertReviewed',
        'history.at': {'$gte': from, '$lt': to},
        //don't count submissions that were inevitably DQ
        'review.inevitable': {$exists : false}
      }
    },
    {
      $project: {
        job: 1,
        recruiter: 1,
        review: 1,
        submittedAt: 1,
        //here is the really accurate filter
        historyArr: {
          $filter: {
            input: '$history',
            as: 'hist',
            cond: {
              $and: [
                {$eq: ['$$hist.status', 'ExpertReviewed']},
                {'$gte': ['$$hist.at', from]},
                {'$lt': ['$$hist.at', to]}
              ]
            }
          }
        }
      }
    },
    {
      $match: {
        historyArr: {$not: {$size: 0}}
      }
    },
    {
      $project: {
        job: 1,
        candidate: 1,
        recruiter: 1,
        review: 1,
        submittedAt: 1,
        pass: {
          $cond: [{$eq: ['$review.pass', true]}, 1, 0]
        },
        history: {$arrayElemAt: ['$historyArr', 0]}
      }
    }];

  let group = {
    $group: {
      _id: {
        'job': '$job'
      },
      total: {$sum: 1},
      pass: {$sum: "$pass"}
    }
  };

  if (!global) {
    group.$group._id.recruiter = '$recruiter';
    aggregate.push(group);
    aggregate.push({
      $sort: {
        '_id.recruiter': 1
      }
    })
  }
  else {
    aggregate.push(group);
  }

  // console.log(params);
  return CandidateSubmission.aggregate(aggregate);
};

/***
 * Compute a recruiter score according to its points, volume of submissions and save his new
 * maximum numbers of slots
 * @param recruiterId {string} Recruiter ID
 * @param totalSubmissions {number} total number of active submissions
 * @param points {number} Recruiter number of points for this time period
 * @return {Promise} A promise that resolves with the updated user or undefined
 */
const computeAndSaveScore = co.wrap(function *(recruiterId, totalSubmissions, points) {
  let score;

  if(points>0) {
    score = GROWTH_FACTOR * points / Math.sqrt(totalSubmissions);
  }
  else {
    score = - DIMINISHING_FACTOR * Math.pow(points,2) / totalSubmissions;
  }
  score = Math.round(score);
  //Do not bypass mongoose otherwise history won't be stored in ScoreHistory
  // yield Preference.update({_id : recruiter}, { $inc: {maxSlots: score}});
  return yield Preference.findOne({_id: recruiterId})
    .then(function (user) {
      if(user) {
        user.recruiter.maxSlots = Math.max(1, user.recruiter.maxSlots + score);
        return user.save();
      }
      else {
        console.error('user ' + recruiterId + ' not found');
      }
    })
    .catch(function (err) {
      console.error(err);
    });
});

/***
 * Loop through all active recruiters for this time period, compare their approval rate to theirs
 * of the other recruiters on the same jobs in order to figure out their points. Then compute their
 * score and deduce the maximum number of slots they will be allowed to submit.
 * @return {Object} the results of the mongodb query (inserted, modified, etc.)
 */
const computeRecruitersScore = co.wrap(function *() {
  let aggregateSubsPerJobs = yield retrieveRecruitersSubmissions(true);

  //figure the total number of submissions and approval rate per job.
  let subsPerJobs = aggregateSubsPerJobs.reduce( (result, subs)  =>{
    result[subs._id.job] = {
      total: subs.total,
      pass: subs.pass,
      approvalRatio: subs.pass / subs.total
    };
    return result;
  }, {});

  let cursorRecruiters = retrieveRecruitersSubmissions().cursor().exec();

  //promisify the mongoose cursor
  const promisifiedNext = Promise.promisify(cursorRecruiters.next).bind(cursorRecruiters);

  let lastRecruiter;
  let totalSubmissions = 0;
  let points = 0;
  let recJobSubs;
  let results={};
  //loop through every couple (recruiter,job) synchronously iterating on the cursor
  do {
    recJobSubs = yield promisifiedNext();
    let recruiter,job;
    if(recJobSubs) {
      recruiter = recJobSubs._id.recruiter;
      job = recJobSubs._id.job;
    }
    if(!lastRecruiter) {
      lastRecruiter=recruiter;
    }
    // If recruiter has changed, compute the score
    if (lastRecruiter !== recruiter) {
      const recruiterScoreUpdates = yield computeAndSaveScore(lastRecruiter, totalSubmissions, points);
      results = {
        inserted: (results.insertedCount || 0) + (recruiterScoreUpdates.insertedCount || 0),
        matchedCount: (results.matchedCount || 0) + (recruiterScoreUpdates.matchedCount || 0),
        modified: (results.modifiedCount || 0) + (recruiterScoreUpdates.modifiedCount || 0),
        deleted: (results.deletedCount || 0) + (recruiterScoreUpdates.deletedCount || 0),
        upserted: (results.upsertedCount || 0) + (recruiterScoreUpdates.upsertedCount || 0)
      };
      totalSubmissions = 0;
      points = 0;
      lastRecruiter = recruiter;
    }
    //If there are submissions for this recruiter, find his delta for this job and sum to his points
    if(recJobSubs) {
      const delta = recJobSubs.pass - (recJobSubs.total * subsPerJobs[job].approvalRatio);
      totalSubmissions += recJobSubs.total;
      points += delta;
    }
  }
  while (recJobSubs != null);
  return results;
});

const RecruitersScoring = {
  run: () => {
    //TODO Time the function execution
    now = new Date();
    console.log('RecruitersScoring run at : ' + now);
    return computeRecruitersScore();
  }
};

module.exports = RecruitersScoring;
